package holon.backend

import holon.*
import holon.Utils.*
import holon.backend.checkpointmanager.{CloudStorageCheckpointManager, DecentralizedCheckpointManager}
import Config.*
import holon.messages.{BroadcastMessage, CRDTUpdate, Checkpoint, ControlMessage, Heartbeat, OwnershipEntry, OwnershipState, OwnershipStateRequest, OwnershipTransferDenial, OwnershipTransferRequest}
import upickle.default.{readBinary, writeBinary}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.immutable.List
import scala.collection.*

class Control(nodeId: Int) {

    private var running = true
    private val jobQueue = new ConcurrentLinkedQueue[Job]()
    private var lastJobQueueCheckTime = 0L

    private var procFunFactory: ProcFunFactory = null
    private var controlState: ControlState = null
    private var controlChannelHandler: ControlChannelHandler = null

    private val producers = mutable.Map.empty[Byte, LogProducer]
    private val out = OutputCollectorImpl(producers)
    private val outputMsgHandler = OutputMessageHandler(out)

    private val checkpointManager = if (USE_CLOUD_STORAGE_CHECKPOINTS) CloudStorageCheckpointManager(out) else DecentralizedCheckpointManager(out)
    private val ownershipManager = PartitionOwnershipManager(nodeId)

    // Failure Detection State
    private val failureDetector = FailureDetector(nodeId, out)
    private var failedNodes = List.empty[Int]

    // Work Stealing State
    private var pollsWithoutRecords = 0
    private var lastWorkStealAttempt = 0L

    // Orphan Node State
    private var orphanNodeStateEnd = 0L

    private val logger = Logger.apply("Recovery")
    Logger.setLevel("Recovery", "INFO")

    RunThread(this.run())

    def submitOrUpdateJob(job: Job): Unit = {
        this.jobQueue.add(job)
    }

    def partitions(): List[Int] = {
        controlState.procFunctionPerPartition.keySet.toList
    }

    private def run(): Unit = {
        while (running) {
            checkJobQueue()
            checkpointManager.createCheckpointIfRequired(nodeId, controlState)
            LagManager.calculateCurrentLagIfRequired(controlState.consumerPerPartition)
            checkNodeFailures()

            runStep()
            Thread.sleep(SLEEP_BETWEEN_POLLS)
        }
    }

    def stop(): Unit = {
        logger.info(s"Stopping node $nodeId")
        this.failureDetector.stop()
        this.running = false
        controlState.consumerPerPartition.values.foreach { case (_, consumer) => consumer.close() }
    }

    private inline def checkJobQueue(): Unit = {
        val t = System.currentTimeMillis()
        if ((t - this.lastJobQueueCheckTime) > 1_000) {
            this.lastJobQueueCheckTime = t

            val job = this.jobQueue.poll()
            if job != null then this.setup(job)
        }
    }

    private def setup(job: Job): Unit = {
        logger.debug(s"Setting up job $job for node $nodeId")
        val basePartitions = job.partitions

        // Setup procFunFactory, consumers and producers
        this.procFunFactory = job.procFunFactory
        this.controlState = new ControlState(procFunFactory, checkpointManager)
        this.controlChannelHandler = new ControlChannelHandler(nodeId, controlState, outputMsgHandler, failureDetector,
                                                               ownershipManager, checkpointManager)
        val (controlChannelConsumer, _) = ConsumerProducerSetup.setupInternalConsumers(job.consumers, controlState.consumerPerPartition)
        ConsumerProducerSetup.setupProducers(job.producers, this.producers)

        // Set initial ownership of partitions (with timestamp as 0)
        ownershipManager.initializePartitionOwnership(basePartitions)
        outputMsgHandler.sendControlMessage(OwnershipState(ownershipManager.getOwnershipMap, nodeId))

        // Recover node state from persistent storage
        val nodeRecoveryFileExists = this.checkpointManager.recoverNodeOffset(nodeId, controlState.consumerPerPartition)
        ownershipManager.setControlChannelConsumer(controlChannelConsumer)

        if (!nodeRecoveryFileExists) {
            logger.info(s"Node $nodeId is starting fresh")
        } else {
            // Request partition ownership state from other nodes
            outputMsgHandler.sendControlMessage(OwnershipStateRequest(nodeId))
            waitForOwnershipStateMessage()

            val partitionsToRequestByOwner = determinePartitionsToRequestOwnership(basePartitions)
            for (ownerNodeId <- partitionsToRequestByOwner.keys) {
                val partitions = partitionsToRequestByOwner(ownerNodeId)
                logger.info(s"Node $nodeId is requesting ownership of partitions $partitions from node $ownerNodeId")
                outputMsgHandler.sendControlMessage(OwnershipTransferRequest(ownerNodeId, partitions, nodeId))
            }
        }

        val partitionsOwned = ownershipManager.getPartitionsOwnedByNode(nodeId)
        logger.info(s"Node $nodeId is responsible for partitions: $partitionsOwned")
        ConsumerProducerSetup.setupPartitionConsumers(job.consumers, partitionsOwned, controlState.consumerPerPartition)
        controlState.setupProcFunctions(partitionsOwned)
        // Recover from the last checkpoint for each partition and node
        this.checkpointManager.recoverPartitionCheckpoints(nodeId, partitionsOwned, controlState)
    }

    private inline def runStep(): Unit = {
        processControlChannel()
        if (controlState.consumerPerPartition.nonEmpty) {
            processNonControlChannels()
        } else {
            logger.info(s"Node $nodeId owns no partitions. Attempting work steal")
            attemptWorkSteal()
        }
        // Flush all producers
        for ((chn, producer) <- producers) do producer.flush()
    }

    /**
     * Wait for an OwnershipState message from another node.
     * If no message is received within 2 seconds, return.
     */
    private def waitForOwnershipStateMessage(): Unit = {
        val t = System.currentTimeMillis()
        while (System.currentTimeMillis() - t < 2_000) {
            val receivedOwnershipState = processControlChannel()
            if (receivedOwnershipState) return

                Thread.sleep(100)
        }
    }

    /**
     * Process control channel messages.
     * Control messages have the highest priority. Process them until there are no more messages.
     *
     * @return true if an OwnershipState message was received, false otherwise
     */
    private def processControlChannel(): Boolean = {
        var receivedOwnershipState = false
        val controlConsumer = controlState.getControlConsumer()
        if (controlConsumer != null) {
            var records = controlConsumer.poll()
            while (records.nonEmpty) {
                logger.debug(s"Node $nodeId is processing control messages")
                for (rec <- records) {
                    val (_, value, _) = rec
                    val message = readBinary[ControlMessage](value)
                    controlChannelHandler.handle(message)

                    if (!receivedOwnershipState && message.isInstanceOf[OwnershipState]) {
                        receivedOwnershipState = true
                    }
                }
                // Poll again, to prioritize control channel processing over other channels
                records = controlConsumer.poll()
            }
        }
        receivedOwnershipState
    }

    /**
     * Process messages from channels other than the Control channel.
     */
    private def processNonControlChannels(): Unit = {
        for ((partitionId, (chn, consumer)) <- controlState.consumerPerPartition) {
            if (chn != CHN_CONTROL) {
                try {
                    val records = consumer.poll()
                    if (records.nonEmpty) {
                        processRecords(chn, partitionId, records)
                    } else if (pollsWithoutRecords >= WORK_STEALING_THRESHOLD) {
                        pollsWithoutRecords = 0
                        attemptWorkSteal()
                    } else {
                        pollsWithoutRecords += 1
                    }
                } catch {
                    case e: IllegalStateException =>
                        if (e.getMessage.contains("This consumer has already been closed.")) {
                            logger.warn(s"Consumer for partition $partitionId is closed. Removing consumer and processing function")
                        } else {
                            logger.error(s"Error processing consumer for partition $partitionId", e)
                        }
                }
            }
        }
    }

    /**
     * Process records from channels other than Control channel.
     */
    private def processRecords(chn: Byte, partitionId: Int, records: LogConsumerRecords): Unit = {
        chn match {
            case CHN_BROADCAST =>
                for (rec <- records) {
                    val (_, value, recTimestamp) = rec
                    val message = readBinary[BroadcastMessage](value)
                    message match {
                        case CRDTUpdate(update, senderId, lag) => {
                            if (senderId != nodeId) {
                                logger.debug(s"Node $nodeId received CRDT update from node $senderId")
                                LagManager.updateLag(senderId, lag)
                            }
                            controlState.procFunctionPerPartition.foreach((_, procFun) =>
                                                                      procFun.processInput(outputFunction, CHN_BROADCAST, Iterable.single((writeBinary(0), update, recTimestamp)))
                                                                  )
                        }
                    }
                }
            case _ =>
                pollsWithoutRecords = 0
                val procFun = controlState.procFunctionPerPartition.getOrElse(partitionId, null)
                if (procFun != null) {
                    procFun.processInput(outputFunction, chn, records)
                }
        }
    }

    /**
     * Output function callback that sends messages to the output channels.
     */
    def outputFunction(partitionId: Int, chn: Byte, recs: LogProducerRecords): Unit = {
        chn match {
            case CHN_BROADCAST =>
                // Add id of current node to the broadcast message for heartbeat tracking
                val currentLag = LagManager.getCurrentLag
                val recordsWithNodeId = recs.map { case (key, value) =>
                    val message = CRDTUpdate(value, nodeId, currentLag)
                    val serializedMessage = writeBinary(message)
                    (key, serializedMessage)
                }
                out.collect(chn, recordsWithNodeId)
            case CHN_OUTPUT =>
                // Check if node is responsible for partition
                val ownerNodeId = ownershipManager.getPartitionOwner(partitionId)
                if (ownerNodeId == nodeId) {
                    out.collect(chn, recs)
                } else {
                    logger.info(s"Node $nodeId cannot output because it is not responsible for partition $partitionId")
                    controlState.dropPartition(partitionId)
                }
            case _ =>
                logger.warn(s"Unknown channel: $chn")
        }
        this.failureDetector.setStartCheckingForFailures()
    }

    /**
     * Check for failed nodes and handle possible failures.
     */
    private def checkNodeFailures(): Unit = {
        // Check for failed nodes & handle failures
        val currentFailedNodes = this.failureDetector.checkNodeFailures()
        if (currentFailedNodes.isDefined) {
            // If all nodes suddenly fail at once, enter orphan state and wait x seconds to see if other nodes recover.
            if (!isOrphanState && N_NODES > 2 && this.failedNodes.isEmpty && currentFailedNodes.get.length == N_NODES - 1) {
                logger.info(s"Suddenly detected all other nodes as failed. Entering orphan state.")
                this.setOrphanStateEnd
            } else if (!isOrphanState) {
                // Only handle nodes when we are not in orphan state
                handleFailedNodes(currentFailedNodes.get.diff(failedNodes))
                this.failedNodes = currentFailedNodes.get
            }
        }
    }

    /**
     * Determine partitions which should be assigned to us but are owned by other nodes.
     * @param basePartitions List of partitions assigned to us.
     * @return Map from owner node id to list of partitions to request ownership for.
     */
    private def determinePartitionsToRequestOwnership(basePartitions: List[Int]): mutable.Map[Int, List[Int]] = {
        val partitionsByOwnerToRequest = mutable.Map.empty[Int, List[Int]]
        var updateOwnershipState = false
        for (partitionId <- basePartitions) {
            val ownerNodeId = ownershipManager.getPartitionOwner(partitionId)

            // If partition is not owned by any node, set ownership to current node
            if (ownerNodeId == -1) {
                updateOwnershipState = true
                ownershipManager.setPartitionOwnership(partitionId)
            } else if (ownerNodeId != nodeId) {
                partitionsByOwnerToRequest(ownerNodeId) = partitionsByOwnerToRequest.getOrElse(ownerNodeId, List.empty) :+ partitionId
            }
        }
        if (updateOwnershipState) outputMsgHandler.sendControlMessage(messages.OwnershipState(ownershipManager.getOwnershipMap, nodeId))
        partitionsByOwnerToRequest
    }

    /**
     * Attempt to steal work from other nodes.
     */
    private def attemptWorkSteal(): Unit = {
        val currentTime = System.currentTimeMillis()
        if (LagManager.getCurrentLag == 0 && (currentTime - lastWorkStealAttempt) > WORK_STEAL_ATTEMPT_COOLDOWN) {
            val victimNode = LagManager.getNodeWithMaxLag
            if (victimNode.isDefined && victimNode.get._2 > 0) {
                logger.info(s"Node $nodeId is attempting work steal from node $victimNode")
                // Send in empty partition list to request any partition
                outputMsgHandler.sendControlMessage(OwnershipTransferRequest(victimNode.get._1, List(), nodeId))
                lastWorkStealAttempt = currentTime
            }
        }
    }

    /**
     * Handle failed nodes by redistributing partitions.
     */
    private def handleFailedNodes(failedNodes: List[Int]): Unit = {
        if (failedNodes.isEmpty) return;

        logger.warn(s"Node $nodeId detected other failed nodes: $failedNodes")

        for failedNode <- failedNodes do
            // Check if current node needs to take over partitions from failed node
            if (this.ownershipManager.checkOwnershipResponsibilityAfterFailure(failedNode, failedNodes)) {
                logger.debug(s"Node $nodeId is responsible for redistribution of partitions from failed node $failedNode")
                val partitions = ownershipManager.getPartitionsOwnedByNode(failedNode)

                // Set new partition ownership for each partition
                for (partitionId <- partitions) {
                    ownershipManager.setPartitionOwnership(partitionId)
                    logger.info(s"Node $nodeId is new owner of partition $partitionId")
                }
                outputMsgHandler.sendControlMessage(messages.OwnershipState(ownershipManager.getOwnershipMap, nodeId))
                logger.info(s"Node $nodeId takes over ownership: ${ownershipManager.getOwnershipMap}")

                controlState.integrateNewPartitions(partitions)
            } else {
                logger.debug(s"Node $nodeId is not responsible for redistribution of partitions from failed node $failedNode")
            }
    }

    private def setOrphanStateEnd: Unit = {
        logger.info(s"Node $nodeId is entering orphan state")
        this.orphanNodeStateEnd = System.currentTimeMillis() + 30_000
    }

    private def isOrphanState: Boolean = {
        System.currentTimeMillis() < this.orphanNodeStateEnd
    }

}
