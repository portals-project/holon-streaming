package holon.backend

import holon.*
import holon.Utils.*
import holon.example.nexmark.Config.*
import upickle.default.{readBinary, writeBinary}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.immutable.List

class Recovery(nodeId: Int) {

    val NODE_ID: Int = nodeId

    private val consumerPerPartition = scala.collection.mutable.Map.empty[Int, (Byte, LogConsumer)]
    private val producers = scala.collection.mutable.Map.empty[Byte, LogProducer]
    private var procFunFactory: ProcFunFactory = null
    private var procFunctionPerPartition = scala.collection.mutable.Map.empty[Int, ProcFun]
    private val out = OutputCollectorImpl(producers)
    private val queue = new ConcurrentLinkedQueue[Job]()
    private val failureDetector = FailureDetector(nodeId)
    private var failedNodes = List.empty[Int]
    private val checkpointManager = CheckpointManager()
    private var pollsWithoutRecords = 0
    private var waitingForWorkStealConfirmationFrom = List.empty[Int]
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

        this.procFunFactory = job.procFunFactory
        setupProducers(job.producers)


        // Request partition ownership from other nodes
        val partitionsByOwnerToRequest = determinePartitionsToRequestOwnership(basePartitions)
        for (ownerNodeId <- partitionsByOwnerToRequest.keys) {
            val partitions = partitionsByOwnerToRequest(ownerNodeId)
            logger.info(s"Node $nodeId is requesting ownership of partitions $partitions from node $ownerNodeId")
            requestPartitionOwnership(ownerNodeId, partitions)
        }

        val partitionsOwned = FirestoreClient.queryPartitionsByNodeId(FirestoreClient.OWNERSHIP_COLLECTION_NAME, nodeId).map(_._1).toList
        logger.info(s"Node $nodeId is responsible for partitions: $partitionsOwned")

        setupConsumers(job.consumers, partitionsOwned)
        logger.debug(s"Consumers: $consumerPerPartition")
        setupProcFunctions(partitionsOwned)

        // Recover from the last checkpoint for each partition and node
        this.checkpointManager.recoverCheckpoint(NODE_ID, partitionsOwned, procFunctionPerPartition, consumerPerPartition)
    }

    private def run(): Unit = {
        var time = 0L

        while (true) {
            // Check the job queue every 1_000 milliseconds
            val t = System.currentTimeMillis()
            if ((t - time) > 1_000) {
                time = t
                checkJobQueue()
            }

            this.checkpointManager.createCheckpointIfRequired(NODE_ID, procFunctionPerPartition, consumerPerPartition)
            LagManager.calculateCurrentLagIfRequired(consumerPerPartition)

            // Check for failed nodes & handle failures
            val currentFailedNodes = failureDetector.checkNodeFailures()
            handleFailedNodes(currentFailedNodes.diff(failedNodes))
            this.failedNodes = currentFailedNodes

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
     * Process control channel messages.
     * Control messages have the highest priority. Process them until there are no more messages.
     */
    private def processControlChannel(): Unit = {
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
                            case OwnershipRequest(receiverId, partitions, senderId) =>
                                if (receiverId == NODE_ID) {
                                    logger.info(s"($nodeId) Received ownership request from $senderId for partitions $partitions")
                                    handoverOwnership(senderId, partitions)

                                    // TODO delete: for now give node more time to recover
                                    if (senderId != nodeId) failureDetector.setHeartbeat(senderId, System.currentTimeMillis() + 5000)
                                }
                            case OwnershipRequestConfirmation(receiverId, partitions, senderId) =>
                                if (receiverId == NODE_ID) {
                                    logger.info(s"(Node $NODE_ID) Received ownership confirmation from node $senderId for partitions $partitions")
                                    if waitingForWorkStealConfirmationFrom.contains(senderId) then
                                        waitingForWorkStealConfirmationFrom = waitingForWorkStealConfirmationFrom.filter(_ != senderId)

                                    // Reset lag for this node after receiving confirmation
                                    LagManager.updateLag(senderId, 0)

                                    integrateNewPartitions(partitions)
                                }
                            case OwnershipRequestDenial(receiverId, partitions, senderId) =>
                                if (receiverId == NODE_ID) {
                                    logger.info(s"(Node $NODE_ID) Received ownership denial from node $senderId for partitions $partitions")
                                    LagManager.updateLag(senderId, 0)
                                    if waitingForWorkStealConfirmationFrom.contains(senderId) then
                                        waitingForWorkStealConfirmationFrom = waitingForWorkStealConfirmationFrom.filter(_ != senderId)
                                }
                        }
                    }
                }
                records = controlConsumer.poll()
            }
        }
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
                    val message = CRDTUpdate(value, NODE_ID, currentLag)
                    val serializedMessage = writeBinary(message)
                    (key, serializedMessage)
                }
                out.collect(chn, recordsWithNodeId)
            case CHN_OUTPUT =>
                // Check if node is responsible for partition
                val (ownerNodeId, _) = FirestoreClient.queryNodeForPartition(FirestoreClient.OWNERSHIP_COLLECTION_NAME, partitionId)
                if ownerNodeId == nodeId then {
                    recs.foreach: r =>
                        val outputState = readBinary[OutputState](r._2)
                        // Deconstruct the output state
                        // val partition = outputState.partition
                        // val windowId = outputState.window
                        val crdtValue = outputState.value

                        logger.info(s"Node $nodeId partition $partitionId commits: $crdtValue")
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
     * Setup producers from producer references.
     */
    private def setupProducers(producerRefs: List[ProducerRef]): Unit = {
        this.producers.clear()
        producerRefs.foreach { ref =>
            val producer = KafkaLogProducer.fromRef(ref)
            this.producers.put(ref.chn, producer)
        }
    }

    /**
     * Setup consumers from consumer references and other partitions that are owned.
     */
    private def setupConsumers(consumerRefs: List[ConsumerRef], partitionsOwned: List[Int]): Unit = {
        this.consumerPerPartition.clear()
        consumerRefs.foreach { ref =>
            if (ref.chn != CHN_NEXMARK || partitionsOwned.contains(ref.partitions.head)) {
                val consumer = KafkaLogConsumer.fromRef(ref)
                logger.debug(s"Node $nodeId - Setting up consumer: $consumer for partition ${consumer.partition}")
                val partition = ref.chn match {
                    case CHN_NEXMARK => consumer.partition
                    case CHN_BROADCAST => BROADCAST_PARTITION_ID
                    case CHN_CONTROL => CONTROL_PARTITION_ID
                    case _ => throw new IllegalArgumentException(s"Unknown channel: ${ref.chn}")
                }

                this.consumerPerPartition.put(partition, (ref.chn, consumer))
            }
        }

        // Setup consumers for other owned partitions
        for (partitionId <- partitionsOwned) {
            if (!this.consumerPerPartition.contains(partitionId)) {
                addNewNexmarkConsumer(partitionId)
            }
        }
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
        for (partitionId <- basePartitions) {
            // Check partition ownership
            val (ownerNodeId, versionNr) = FirestoreClient.queryNodeForPartition(FirestoreClient.OWNERSHIP_COLLECTION_NAME, partitionId)

            // If partition is not owned by any node, set ownership to current node
            if (ownerNodeId == -1) {
                FirestoreClient.setPartitionOwnership(partitionId, nodeId, 0)
            } else if (ownerNodeId != nodeId) {
                partitionsByOwnerToRequest(ownerNodeId) = partitionsByOwnerToRequest.getOrElse(ownerNodeId, List.empty) :+ partitionId
            }
        }
        partitionsByOwnerToRequest
    }

    private def attemptWorkSteal(): Unit = {
        if (LagManager.getCurrentLag == 0 && waitingForWorkStealConfirmationFrom.isEmpty) {
            val victimNode = LagManager.getNodeWithMaxLag
            if (victimNode.isDefined && victimNode.get._2 > 0) {
                logger.info(s"Node $nodeId is attempting work steal from node $victimNode")
                waitingForWorkStealConfirmationFrom = List(victimNode.get._1)
                // Send in empty partition list to request any partition
                requestPartitionOwnership(victimNode.get._1, List())
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
            if (checkFailureRedistributionResponsibility(failedNode, failedNodes)) {
                logger.debug(s"Node $nodeId is responsible for redistribution of partitions from failed node $failedNode")
                val partitions = FirestoreClient.queryPartitionsByNodeId(FirestoreClient.OWNERSHIP_COLLECTION_NAME, failedNode)

                // Set new partition ownership for each partition
                for (partitionId, versionNr) <- partitions do
                    FirestoreClient.setPartitionOwnership(partitionId, nodeId, versionNr + 1)
                    logger.info(s"Node $nodeId is new owner of partition $partitionId")

                integrateNewPartitions(partitions.map(_._1))
            } else {
                logger.debug(s"Node $nodeId is not responsible for redistribution of partitions from failed node $failedNode")
            }
    }

    /**
     * Integrate new partitions into the system. Create processing function, consumer & restore snapshot.
     */
    private def integrateNewPartitions(partitions: List[Int]): Unit = {
        for (partitionId <- partitions) {
            procFunctionPerPartition += partitionId -> this.procFunFactory.create(partitionId)
            addNewNexmarkConsumer(partitionId)

            this.checkpointManager.recoverCheckpointForPartition(partitionId, procFunctionPerPartition(partitionId),
                                                                 consumerPerPartition(partitionId)._2)
        }
    }

    /**
     * Add new Nexmark consumer for the partition.
     */
    private def addNewNexmarkConsumer(partitionId: Int): Unit = {
        val inputConsumer = KafkaLogConsumer.fromRef(ConsumerRef(
            chn = CHN_NEXMARK,
            host = KAFKA_HOST,
            port = KAFKA_PORT,
            topic = KAFKA_TOPIC_NEXMARK,
            partitions = List(partitionId),
            ))
        logger.debug(s"Node $nodeId - Adding new consumer for partition $partitionId: $inputConsumer")
        this.consumerPerPartition.put(partitionId, (CHN_NEXMARK, inputConsumer))
    }

    /**
     * Check if the current node is responsible for taking over the partitions of the failed node.
     */
    private def checkFailureRedistributionResponsibility(failedNode: Int, failedNodes: List[Int]): Boolean = {
        // Get new owner for the partitions
        var owner = failedNode
        while failedNodes.contains(failedNode) && owner != nodeId do
        // If the failed node is also the current node, then the current node is responsible for redistribution
            owner = (failedNode + 1) % N_NODES

        owner == nodeId
    }

    /**
     * Request partition ownership from the current owner node.
     * Partition list can be empty to request any partition.
     */
    private def requestPartitionOwnership(ownerNodeId: Int, partitions: List[Int]): Unit = {
        // Create the OwnershipRequest message
        val message = OwnershipRequest(ownerNodeId, partitions, NODE_ID)

        // Serialize the message
        val serializedMessage = writeBinary(message)

        // Send the serialized message
        val records = List((writeBinary(0), serializedMessage))
        out.collect(CHN_CONTROL, records)
        logger.info(s"Requesting ownership of partitions $partitions from node $ownerNodeId")
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
                sendOwnershipHandoverDenial(newOwnerId, partitions)
                return
            }
            logger.info(s"Node $nodeId is handing over partition $partitionIdAndLag to node $newOwnerId. It has the largest lag")
            partitionsToHandover = List(partitionIdAndLag.get._1)
        }

        // Do not hand over last partition, otherwise this node becomes idle!
        val ownedPartitions = this.procFunctionPerPartition.keySet.toList
        if (partitionsToHandover.size == ownedPartitions.size) {
            logger.info(s"Node $nodeId cannot hand over all partitions to node $newOwnerId. At least one partition must remain")
            sendOwnershipHandoverDenial(newOwnerId, partitions)
            return
        }

        // Checkpoint current state, set new owner & close consumer
        var transferredPartitions = List.empty[Int]
        for partitionId <- partitionsToHandover do
            if (this.procFunctionPerPartition.keySet.contains(partitionId)) {

                val (chn, consumer) = this.consumerPerPartition(partitionId)
                val procFun = this.procFunctionPerPartition(partitionId)
                this.checkpointManager.createCheckpointForPartition(partitionId, procFun, consumer)
                FirestoreClient.setPartitionOwnership(partitionId, newOwnerId, 0)

                removeConsumerAndProcFun(partitionId)
                transferredPartitions = transferredPartitions :+ partitionId
            }

        logger.info(s"Partition $transferredPartitions ownership handed over to node $newOwnerId")

        // Send ownership confirmation to new owner
        val message = OwnershipRequestConfirmation(newOwnerId, transferredPartitions, NODE_ID)
        val serializedMessage = writeBinary(message)
        val records = List((writeBinary(0), serializedMessage))
        out.collect(CHN_CONTROL, records)
    }

    private def sendOwnershipHandoverDenial(newOwnerId: Int, transferredPartitions: List[Int]): Unit = {
        val message = OwnershipRequestDenial(newOwnerId, transferredPartitions, NODE_ID)
        val serializedMessage = writeBinary(message)
        val records = List((writeBinary(0), serializedMessage))
        out.collect(CHN_CONTROL, records)
    }

    private def removeConsumerAndProcFun(partitionId: Int): Unit = {
        logger.info(s"Node $nodeId is removing consumer and processing function for partition $partitionId")
        val (_chn, consumer) = this.consumerPerPartition(partitionId)
        logger.info(s"Node $nodeId is closing consumer for partition $partitionId wih channel $_chn and consumer $consumer")
        consumer.close()
        this.consumerPerPartition.remove(partitionId)
        this.procFunctionPerPartition.remove(partitionId)
    }

}
