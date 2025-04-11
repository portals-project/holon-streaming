package holon.backend

import holon.*
import holon.Utils.*
import holon.backend.checkpointmanager.{CloudStorageCheckpointManager, DecentralizedCheckpointManager}
import holon.backend.messages.{BroadcastMessage, CRDTUpdate, ControlMessage, Checkpoint, OwnershipState, OwnershipStateRequest, OwnershipTransferDenial, OwnershipTransferRequest}
import Config.*
import upickle.default.{readBinary, writeBinary}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.immutable.List

class Recovery(nodeId: Int) {

    private val consumerPerPartition = scala.collection.mutable.Map.empty[Int, (Byte, LogConsumer)]
    private val producers = scala.collection.mutable.Map.empty[Byte, LogProducer]
    private var procFunFactory: ProcFunFactory = null
    private var procFunctionPerPartition = scala.collection.mutable.Map.empty[Int, ProcFun]
    private val out = OutputCollectorImpl(producers)
    private val queue = new ConcurrentLinkedQueue[Job]()
    private val failureDetector = FailureDetector(nodeId)
    private var failedNodes = List.empty[Int]
    private val checkpointManager = if (USE_CLOUD_STORAGE_CHECKPOINTS) CloudStorageCheckpointManager() else DecentralizedCheckpointManager(out)
    private var pollsWithoutRecords = 0
    private var waitingForWorkStealConfirmationFrom = List.empty[Int] // TODO: likely delete
    private val ownershipManager = PartitionOwnershipManager(nodeId)
    private val logger = Logger.apply("Recovery")

    Logger.setLevel("Recovery", "INFO")

    RunThread(this.run())

    def submitOrUpdate(job: Job): Unit = {
        this.queue.add(job)
    }

    private def setup(job: Job): Unit = {
        // Partitions node should probably hold based on its id
        val basePartitions = job.partitions
        logger.debug(s"Setting up job $job for node $nodeId")

        // Setup procFunFactory, consumers and producers
        this.procFunFactory = job.procFunFactory
        consumerPerPartition.clear()
        val (controlChannelConsumer, _) = ConsumerProducerSetup.setupInternalConsumers(job.consumers, consumerPerPartition)
        ConsumerProducerSetup.setupProducers(job.producers, this.producers)

        // Set initial ownership of partitions
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

            val partitionsByOwnerToRequest = determinePartitionsToRequestOwnership(basePartitions)
            for (ownerNodeId <- partitionsByOwnerToRequest.keys) {
                val partitions = partitionsByOwnerToRequest(ownerNodeId)
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
        var time = 0L

        while (true) {
            // Check the job queue every 1_000 milliseconds
            val t = System.currentTimeMillis()
            if ((t - time) > 1_000) {
                time = t
                checkJobQueue()
                logger.debug(s"Node $nodeId ownership: ${ownershipManager.getOwnershipMap}")
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
        processOtherChannels()
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
                    val (_, value) = rec
                    val message = readBinary[ControlMessage](value)

                    if (message.senderId != nodeId) {
                        failureDetector.setHeartbeat(message.senderId)

                        message match {
                            case Checkpoint(senderId, partitionSnapshots) =>
                                // Node Checkpoints are only send when checkpoints are not saved to cloud storage
                                if (senderId != nodeId) {
                                    logger.debug(s"Node $nodeId received checkpoint from node $senderId")
                                    failureDetector.setHeartbeat(senderId)
                                    this.checkpointManager.saveSnapshotsFromOtherNodes(partitionSnapshots)
                                }
                            case OwnershipState(ownershipMap, senderId) =>
                                logger.debug(s"($nodeId) Received ownership state from node $senderId: $ownershipMap")
                                if waitingForWorkStealConfirmationFrom.contains(senderId) then
                                    waitingForWorkStealConfirmationFrom = waitingForWorkStealConfirmationFrom.filter(_ != senderId)

                                val newPartitions = ownershipManager.getNewOwnedPartitions(ownershipMap)
                                integrateNewPartitions(newPartitions)
                                ownershipManager.mergeOwnershipMap(ownershipMap)
                                receivedOwnershipState = true
                                failureDetector.setHeartbeat(senderId)
                            case OwnershipStateRequest(senderId) =>
                                logger.info(s"($nodeId) Received ownership state request from node $senderId")
                                this.checkpointManager.sendCheckpointMessage(nodeId)
                                val ownershipMap = ownershipManager.getOwnershipMap
                                sendControlMessage(OwnershipState(ownershipMap, nodeId))
                            case OwnershipTransferRequest(receiverId, partitions, senderId) =>
                                if (receiverId == nodeId) {
                                    logger.info(s"($nodeId) Received ownership request from $senderId for partitions $partitions")
                                    handoverOwnership(senderId, partitions)

                                    // TODO delete: for now give node more time to recover
                                    failureDetector.setHeartbeat(senderId, System.currentTimeMillis() + 5000)
                                }
                            case OwnershipTransferDenial(receiverId, partitions, senderId) =>
                                if (receiverId == nodeId) {
                                    logger.info(s"(Node $nodeId) Received ownership denial from node $senderId for partitions $partitions")
                                    LagManager.updateLag(senderId, 0)
                                    if waitingForWorkStealConfirmationFrom.contains(senderId) then
                                        waitingForWorkStealConfirmationFrom = waitingForWorkStealConfirmationFrom.filter(_ != senderId)
                                    failureDetector.setHeartbeat(senderId)
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
                    logger.debug(s"Node $nodeId is polling partition $partitionId from channel $chn: Consumer: $consumer - Records nonEmpty: ${records.nonEmpty}")

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
    private def processRecords(chn: Byte, partitionId: Int, records: Iterable[(Array[Byte], Array[Byte])]): Unit = {
        chn match {
            case CHN_BROADCAST =>
                for (rec <- records) {
                    val (_, value) = rec
                    val message = readBinary[BroadcastMessage](value)
                    message match {
                        case CRDTUpdate(update, senderId, lag) => {
                            if (senderId != nodeId) {
                                logger.debug(s"Node $nodeId received CRDT update from node $senderId")
                                failureDetector.setHeartbeat(senderId)
                                LagManager.updateLag(senderId, lag)
                            }
                            this.procFunctionPerPartition.foreach((_, procFun) =>
                                                                      procFun.process(outputFunction, CHN_BROADCAST, Iterable.single((writeBinary(0), update)))
                                                                  )
                        }
                    }
                }
            case _ =>
                pollsWithoutRecords = 0
                val procFun = this.procFunctionPerPartition(partitionId)
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
                if ownerNodeId == nodeId then {
                    recs.foreach: r =>
                        val outputState = readBinary[OutputState](r._2)
                        // Deconstruct the output state
                        // val partition = outputState.partition
                        // val windowId = outputState.window
                        val crdtValue = outputState.bidCount

//                        logger.info(s"Node $nodeId partition $partitionId commits: $crdtValue")

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

    private def attemptWorkSteal(): Unit = {
        if (LagManager.getCurrentLag == 0 && waitingForWorkStealConfirmationFrom.isEmpty) {
            val victimNode = LagManager.getNodeWithMaxLag
            if (victimNode.isDefined && victimNode.get._2 > 0) {
                logger.info(s"Node $nodeId is attempting work steal from node $victimNode")
                waitingForWorkStealConfirmationFrom = List(victimNode.get._1)
                // Send in empty partition list to request any partition
                sendControlMessage(OwnershipTransferRequest(victimNode.get._1, List(), nodeId))
            }
        }
    }

    /**
     * Handle failed nodes by redistributing partitions.
     */
    private def handleFailedNodes(failedNodes: List[Int]): Unit = {
        if (failedNodes.isEmpty) return;

        // Remove failed nodes from waitingForWorkStealConfirmationFrom list
        waitingForWorkStealConfirmationFrom = waitingForWorkStealConfirmationFrom.filterNot(failedNodes.contains)

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
     * Handover ownership of partitions to a new owner.
     * If the partition list is empty, hand over partition with largest lag.
     * Checkpoint current state, set new owner & close consumer.
     * Send ownership confirmation to new owner.
     */
    private def handoverOwnership(newOwnerId: Int, partitions: List[Int]): Unit = {
        var partitionsToHandover = partitions
        if (partitionsToHandover.isEmpty) {
            val partitionIdAndLag = LagManager.getPartitionWithMaxLag(consumerPerPartition)
            if (partitionIdAndLag.isEmpty || partitionIdAndLag.get._2 <= 0) {
                logger.info(s"Node $nodeId cannot hand over any partitions to node $newOwnerId. No partitions with lag")
                sendControlMessage(OwnershipTransferDenial(newOwnerId, partitions, nodeId))
                return
            }
            logger.info(s"Node $nodeId is handing over partition $partitionIdAndLag to node $newOwnerId. It has the largest lag")
            partitionsToHandover = List(partitionIdAndLag.get._1)
        }

        // Do not hand over last partition, otherwise this node becomes idle!
        val ownedPartitions = this.procFunctionPerPartition.keySet.toList
        if (partitionsToHandover.size == ownedPartitions.size) {
            logger.info(s"Node $nodeId cannot hand over all partitions to node $newOwnerId. At least one partition must remain")
            sendControlMessage(OwnershipTransferDenial(newOwnerId, partitions, nodeId))
            return
        }

        // Checkpoint current state, set new owner & close consumer
        var transferredPartitions = List.empty[Int]
        for (partitionId <- partitionsToHandover) {
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
        logger.info(s"Node $nodeId is removing consumer and processing function for partition $partitionId")
        val (_chn, consumer) = this.consumerPerPartition(partitionId)
        logger.info(s"Node $nodeId is closing consumer for partition $partitionId wih channel $_chn and consumer $consumer")
        consumer.close()
        this.consumerPerPartition.remove(partitionId)
        this.procFunctionPerPartition.remove(partitionId)
    }

    private def sendControlMessage(message: ControlMessage): Unit = {
        val serializedMessage = writeBinary(message)
        val records = List((writeBinary(0), serializedMessage))
        out.collect(CHN_CONTROL, records)
    }

}
