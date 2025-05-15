package holon.backend

import holon.*
import holon.Utils.*
import holon.backend.checkpointmanager.{CloudStorageCheckpointManager, DecentralizedCheckpointManager}
import holon.backend.messages.*
import Config.*
import upickle.default.{readBinary, writeBinary}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.immutable.List

class Recovery(nodeId: Int) {

    private var running = true
    private val queue = new ConcurrentLinkedQueue[Job]()
    private val producers = scala.collection.mutable.Map.empty[Byte, LogProducer]
    private val consumerPerPartition = scala.collection.mutable.Map.empty[Int, (Byte, LogConsumer)]
    private val out = OutputCollectorImpl(producers)
    private var procFunFactory: ProcFunFactory = null
    private var procFunctionPerPartition = scala.collection.mutable.Map.empty[Int, ProcFun]

    private val checkpointManager = if (USE_CLOUD_STORAGE_CHECKPOINTS) CloudStorageCheckpointManager(out) else DecentralizedCheckpointManager(out)
    private val ownershipManager = PartitionOwnershipManager(nodeId)

    private val failureDetector = FailureDetector(nodeId, out)
    private var failedNodes = List.empty[Int]

    private var pollsWithoutRecords = 0
    private var lastWorkStealAttempt = 0L

    private val logger = Logger.apply("Recovery")
    Logger.setLevel("Recovery", "INFO")

    RunThread(this.run())

    def submitOrUpdate(job: Job): Unit = {
        this.queue.add(job)
    }

    def partitions(): List[Int] = {
        this.procFunctionPerPartition.keySet.toList
    }

    def stop(): Unit = {
        logger.info(s"Stopping node $nodeId")
        this.failureDetector.stop()
        this.running = false
        this.consumerPerPartition.values.foreach { case (_, consumer) => consumer.close() }
    }

    private def setup(job: Job): Unit = {
        logger.debug(s"Setting up job $job for node $nodeId")
        val basePartitions = job.partitions

        // Setup procFunFactory, consumers and producers
        this.procFunFactory = job.procFunFactory
        consumerPerPartition.clear()
        val (controlChannelConsumer, _) = ConsumerProducerSetup.setupInternalConsumers(job.consumers, consumerPerPartition)
        ConsumerProducerSetup.setupProducers(job.producers, this.producers)

        // Set initial ownership of partitions (with timestamp as 0)
        ownershipManager.initializePartitionOwnership(basePartitions)
        sendControlMessage(OwnershipState(ownershipManager.getOwnershipMap, nodeId))

        // Recover node state from persistent storage
        val nodeRecoveryFileExists = this.checkpointManager.recoverNodeOffset(nodeId, consumerPerPartition)
        ownershipManager.setControlChannelConsumer(controlChannelConsumer)

        if (!nodeRecoveryFileExists) {
            logger.info(s"Node $nodeId is starting fresh")
        } else {
            // Request partition ownership state from other nodes
            sendControlMessage(OwnershipStateRequest(nodeId))
            waitForOwnershipStateMessage()

            val partitionsToRequestByOwner = determinePartitionsToRequestOwnership(basePartitions)
            for (ownerNodeId <- partitionsToRequestByOwner.keys) {
                val partitions = partitionsToRequestByOwner(ownerNodeId)
                logger.info(s"Node $nodeId is requesting ownership of partitions $partitions from node $ownerNodeId")
                sendControlMessage(OwnershipTransferRequest(ownerNodeId, partitions, nodeId))
            }
        }

        val partitionsOwned = ownershipManager.getPartitionsOwnedByNode(nodeId)
        logger.info(s"Node $nodeId is responsible for partitions: $partitionsOwned")
        ConsumerProducerSetup.setupPartitionConsumers(job.consumers, partitionsOwned, consumerPerPartition)
        setupProcFunctions(partitionsOwned)
        // Recover from the last checkpoint for each partition and node
        this.checkpointManager.recoverPartitionCheckpoints(nodeId, partitionsOwned, procFunctionPerPartition, consumerPerPartition)
    }

    private def run(): Unit = {
        var lastJobQueueCheckTime = 0L

        while (running) {
            // Check the job queue every 1_000 milliseconds
            val t = System.currentTimeMillis()
            if ((t - lastJobQueueCheckTime) > 1_000) {
                lastJobQueueCheckTime = t
                checkJobQueue()
            }

            this.checkpointManager.createCheckpointIfRequired(nodeId, procFunctionPerPartition, consumerPerPartition)
            LagManager.calculateCurrentLagIfRequired(consumerPerPartition)

            // Check for failed nodes & handle failures
            val currentFailedNodes = failureDetector.checkNodeFailures()
            if (currentFailedNodes.isDefined) {
                handleFailedNodes(currentFailedNodes.get.diff(failedNodes))
                this.failedNodes = currentFailedNodes.get
            }

            runStep()
            Thread.sleep(SLEEP_BETWEEN_POLLS)
        }
    }

    private inline def checkJobQueue(): Unit = {
        val job = this.queue.poll()
        if job != null then this.setup(job)
    }

    private inline def runStep(): Unit = {
        processControlChannel()
        if (this.consumerPerPartition.nonEmpty) {
            processOtherChannels()
        } else {
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

        val (_, controlConsumer) = this.consumerPerPartition.getOrElse(CONTROL_PARTITION_ID, (0, null))
        if (controlConsumer != null) {
            var records = controlConsumer.poll()
            while (records.nonEmpty) {
                logger.debug(s"Node $nodeId is processing control messages")
                for (rec <- records) {
                    val (_, value, _) = rec
                    val message = readBinary[ControlMessage](value)

                    if (message.senderId != nodeId) {
                        message match {
                            case Heartbeat(senderId) =>
                                failureDetector.setHeartbeat(senderId)
                            case Checkpoint(senderId, partitionSnapshots) =>
                                // Node Checkpoints are only send when checkpoints are not saved to cloud storage
                                if (senderId != nodeId) {
                                    logger.debug(s"Node $nodeId received checkpoint from node $senderId")
                                    this.checkpointManager.saveSnapshotsFromOtherNodes(partitionSnapshots)
                                    checkIfCheckpointIncludesOwnPartitions(senderId, partitionSnapshots)
                                }
                            case OwnershipState(ownershipMap, senderId) =>
                                logger.debug(s"($nodeId) Received ownership state from node $senderId: $ownershipMap")

                                val newPartitions = ownershipManager.getNewOwnedPartitions(ownershipMap)
                                integrateNewPartitions(newPartitions)
                                ownershipManager.mergeOwnershipMap(ownershipMap)
                                receivedOwnershipState = true
                            case OwnershipStateRequest(senderId) =>
                                logger.info(s"($nodeId) Received ownership state request from node $senderId")
                                this.checkpointManager.sendCheckpointMessage(nodeId)
                                val ownershipMap = ownershipManager.getOwnershipMap
                                sendControlMessage(OwnershipState(ownershipMap, nodeId))
                            case OwnershipTransferRequest(receiverId, partitions, senderId) =>
                                if (receiverId == nodeId) {
                                    logger.info(s"($nodeId) Received ownership request from $senderId for partitions $partitions")
                                    handleOwnershipTransferRequest(senderId, partitions)
                                }
                            case OwnershipTransferDenial(receiverId, partitions, senderId) =>
                                if (receiverId == nodeId) {
                                    logger.info(s"(Node $nodeId) Received ownership denial from node $senderId for partitions $partitions")
                                    LagManager.updateLag(senderId, 0)
                                }
                        }
                    }
                }
                records = controlConsumer.poll()
            }
        }
        receivedOwnershipState
    }

    /**
     * Process messages from channels other than the Control channel.
     */
    private def processOtherChannels(): Unit = {
        for ((partitionId, (chn, consumer)) <- this.consumerPerPartition) {
            if (chn != CHN_CONTROL) {
                try {
                    val records = consumer.poll()

                    if (records.nonEmpty) {
                        processRecords(chn, partitionId, records)
                    } else if (pollsWithoutRecords >= WORK_STEALING_THRESHOLD) {
                        logger.debug(s"Node $nodeId - partition $partitionId received no records from channel $chn")
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
    private def processRecords(chn: Byte, partitionId: Int, records: Iterable[(Array[Byte], Array[Byte], Long)]): Unit = {
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
                            this.procFunctionPerPartition.foreach((_, procFun) =>
                                                                      procFun.process(outputFunction, CHN_BROADCAST, Iterable.single((writeBinary(0), update, recTimestamp)))
                                                                  )
                        }
                    }
                }
            case _ =>
                pollsWithoutRecords = 0
                val procFun = this.procFunctionPerPartition.getOrElse(partitionId, null)
                if (procFun != null) {
                    logger.debug(s"Node $nodeId is processing records for partition $partitionId")
                    procFun.process(outputFunction, chn, records)
                }
        }
    }

    /**
     * Output function callback that send messages to the output channels.
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
                    //requestPartitionOwnership(ownerNodeId, List(partitionId))
                    removeConsumerAndProcFun(partitionId)
                }
            case _ =>
                logger.warn(s"Unknown channel: $chn")
                logger.info(s"$chn: $recs")
        }
        this.failureDetector.setStartCheckingForFailures()
    }

    /**
     * Setup processing functions for each partition owned.
     */
    private def setupProcFunctions(partitionsOwned: List[Int]): Unit = {
        this.procFunctionPerPartition = scala.collection.mutable.Map(partitionsOwned.map { partition =>
            partition -> this.procFunFactory.create(partition)
        }: _*)
    }

    /**
     * Determine partitions which should be assigned to us but are owned by other nodes.
     *
     * @param basePartitions List of partitions assigned to us.
     * @return Map from owner node id to list of partitions to request ownership for.
     */
    private def determinePartitionsToRequestOwnership(basePartitions: List[Int]): scala.collection.mutable.Map[Int, List[Int]] = {
        val partitionsByOwnerToRequest = scala.collection.mutable.Map.empty[Int, List[Int]]
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
        if (updateOwnershipState) sendControlMessage(OwnershipState(ownershipManager.getOwnershipMap, nodeId))
        partitionsByOwnerToRequest
    }

    /**
     * Attempt to steal work from other nodes if there is no lag and no confirmation is pending.
     */
    private def attemptWorkSteal(): Unit = {
        val currentTime = System.currentTimeMillis()
        if (LagManager.getCurrentLag == 0 && (currentTime - lastWorkStealAttempt) > WORK_STEAL_ATTEMPT_COOLDOWN) {
            val victimNode = LagManager.getNodeWithMaxLag
            if (victimNode.isDefined && victimNode.get._2 > 0) {
                logger.info(s"Node $nodeId is attempting work steal from node $victimNode")
                // Send in empty partition list to request any partition
                sendControlMessage(OwnershipTransferRequest(victimNode.get._1, List(), nodeId))
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
                sendControlMessage(OwnershipState(ownershipManager.getOwnershipMap, nodeId))
                logger.info(s"Node $nodeId takes over ownership: ${ownershipManager.getOwnershipMap}")

                integrateNewPartitions(partitions)
            } else {
                logger.debug(s"Node $nodeId is not responsible for redistribution of partitions from failed node $failedNode")
            }
    }

    /**
     * Integrate new partitions into the system. Create processing function, consumer & restore snapshot.
     */
    private def integrateNewPartitions(partitions: List[Int]): Unit = {
        for (partitionId <- partitions) {
            val procFun = this.procFunFactory.create(partitionId)
            procFunctionPerPartition += partitionId -> procFun
            val consumer = ConsumerProducerSetup.addNewInputConsumer(partitionId, consumerPerPartition)

            this.checkpointManager.recoverCheckpointForPartition(partitionId, procFun, consumer)
        }
    }

    /**
     * Check if the checkpoint received from another node includes partitions owned by this node.
     * If so, check if the offset is greater than the current offset for this partition.
     * If it is, hand over ownership of the partition to the sender node.
     */
    private def checkIfCheckpointIncludesOwnPartitions(senderId: Int, partitionSnapshots: Map[Int, (Long, String)]): Unit = {
        // TODO: Maybe drop
        for ((partitionId, (snapshotOffset, _)) <- partitionSnapshots) {
            if (this.procFunctionPerPartition.contains(partitionId)) {
                logger.info(s"Node $nodeId received checkpoint for partition $partitionId which it owns from node $senderId")

                // Compare offsets
                val consumer = this.consumerPerPartition(partitionId)._2
                if (consumer.offsets().head._2 < snapshotOffset) {
                    logger.info(s"Node $nodeId received checkpoint for partition $partitionId with offset $snapshotOffset " +
                                    s"which is greater than current offset ${consumer.offsets().head._2} for this partition. Dropping partition!")
                    ownershipManager.setPartitionOwnership(partitionId, senderId)
                    removeConsumerAndProcFun(partitionId)
                }
            }
        }
        sendControlMessage(OwnershipState(ownershipManager.getOwnershipMap, nodeId))
    }

    /**
     * Handle ownership transfer request from other nodes.
     * If the partition list is empty, hand over the partition with largest lag. Else hand over the requested partitions.
     * Deny ownership transfer if all owned partitions are requested or there are no partitions with lag.
     */
    private def handleOwnershipTransferRequest(newOwnerId: Int, partitions: List[Int]): Unit = {
        var partitionsToHandover = partitions
        // If the partition list is empty (in case of work stealing), hand over partition with largest lag.
        if (partitionsToHandover.isEmpty) {
            val partitionIdAndLag = LagManager.getPartitionWithMaxLag(consumerPerPartition)
            if (partitionIdAndLag.isEmpty || partitionIdAndLag.get._2 <= 0) {
                logger.info(s"Node $nodeId cannot hand over any partitions to node $newOwnerId. No partitions with lag")
                sendControlMessage(OwnershipTransferDenial(newOwnerId, partitions, nodeId))
                return
            }
            logger.info(s"Detected $partitionIdAndLag as partition with largest lag.")
            partitionsToHandover = List(partitionIdAndLag.get._1)
        }

        // Do not hand over last partition, otherwise this node becomes idle!
        val ownedPartitions = this.procFunctionPerPartition.keySet.toList
        if (partitionsToHandover.size == ownedPartitions.size) {
            logger.info(s"Node $nodeId cannot hand over all partitions to node $newOwnerId. At least one partition must remain")
            sendControlMessage(OwnershipTransferDenial(newOwnerId, partitions, nodeId))
            return
        }

        handoverOwnership(newOwnerId, partitionsToHandover)
    }

    /**
     * Handover ownership of partitions to a new owner.
     * Checkpoint current state, set new owner & close consumer.
     * Send ownership confirmation to new owner.
     */
    private def handoverOwnership(newOwnerId: Int, partitions: List[Int]): Unit = {
        // Checkpoint current state, set new owner & close consumer
        var transferredPartitions = List.empty[Int]
        for (partitionId <- partitions) {
            if (this.procFunctionPerPartition.keySet.contains(partitionId)) {
                val (chn, consumer) = this.consumerPerPartition(partitionId)
                val procFun = this.procFunctionPerPartition(partitionId)
                this.checkpointManager.createCheckpointForPartition(nodeId, partitionId, procFun, consumer)
                ownershipManager.setPartitionOwnership(partitionId, newOwnerId)

                removeConsumerAndProcFun(partitionId)
                transferredPartitions = transferredPartitions :+ partitionId
            }
        }

        // Send new checkpoint state
        this.checkpointManager.sendCheckpointMessage(nodeId)
        // Notify other nodes about updated ownership
        sendControlMessage(OwnershipState(ownershipManager.getOwnershipMap, nodeId))
        logger.info(s"Partition $transferredPartitions ownership handed over to node $newOwnerId")
        logger.info(s"Node $nodeId handed over ownership: ${ownershipManager.getOwnershipMap}")
    }

    private def removeConsumerAndProcFun(partitionId: Int): Unit = {
        val (_chn, consumer) = this.consumerPerPartition(partitionId)
        if (consumer == null) {
            logger.warn(s"Consumer for partition $partitionId is null. Cannot remove consumer and processing function")
            return
        }
        consumer.close()
        if this.procFunctionPerPartition.contains(partitionId) then
            this.procFunctionPerPartition.remove(partitionId)

        if this.consumerPerPartition.contains(partitionId) then
            this.consumerPerPartition.remove(partitionId)
    }

    private def sendControlMessage(message: ControlMessage): Unit = {
        val serializedMessage = writeBinary(message)
        val records = List((writeBinary(0), serializedMessage))
        out.collect(CHN_CONTROL, records)
    }

}
