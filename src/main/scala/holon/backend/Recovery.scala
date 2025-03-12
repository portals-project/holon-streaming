package holon.backend

import holon.*
import holon.Utils.*
import holon.example.nexmark.Config.*
import upickle.default.{readBinary, writeBinary}

import java.util.concurrent.ConcurrentLinkedQueue

class Recovery(nodeId: Int) {

    val NODE_ID: Int = nodeId

    private val BROADCAST_PARTITION_ID = -1
    private var partitions: List[Int] = List.empty
    private val consumerPerPartition = scala.collection.mutable.Map.empty[Int, (Byte, LogConsumer)]
    private val producers = scala.collection.mutable.Map.empty[Byte, LogProducer]
    private var procFunctionPerPartition = scala.collection.mutable.Map.empty[Int, ProcFun]
    private val out = OutputCollectorImpl(producers)
    private val queue = new ConcurrentLinkedQueue[Job]()
    private val failureDetector = FailureDetector(nodeId)
    private var failedNodes = List.empty[Int]
    private val checkpointManager = CheckpointManager()
    private val logger = Logger.apply("Recovery")

    Logger.setLevel("Recovery", "INFO")

    RunThread(this.run())

    def submitOrUpdate(job: Job): Unit = {
        this.queue.add(job)
    }

    private def setup(job: Job): Unit = {
        // Partitions node should probably hold based on its id
        val basePartitions = job.partitions

        logger.info(s"Setting up job for node $nodeId")
        // Setup producers
        this.producers.clear()
        job.producers.foreach: ref =>
            val producer = KafkaLogProducer.fromRef(ref)
            this.producers.put(ref.chn, producer)

        val partitionsByOwnerToRequest = scala.collection.mutable.Map.empty[Int, List[Int]]
        for (partitionId <- basePartitions) {
            // Check partition ownership
            val (ownerNodeId, versionNr) = FirestoreClient.queryNodeForPartition(FirestoreClient.OWNERSHIP_COLLECTION_NAME, partitionId)

            // If partition is not owned by any node, set ownership to current node
            if ownerNodeId == -1 then
                FirestoreClient.setPartitionOwnership(partitionId, nodeId, 0)

            else if ownerNodeId != nodeId then
            // Add partition to request list
                if partitionsByOwnerToRequest.contains(ownerNodeId) then
                    partitionsByOwnerToRequest(ownerNodeId) = partitionsByOwnerToRequest(ownerNodeId) :+ partitionId
                else
                    partitionsByOwnerToRequest(ownerNodeId) = List(partitionId)
        }

        // Get all partitions owned by current node
        val partitionsOwned = FirestoreClient.queryPartitionsByNodeId(FirestoreClient.OWNERSHIP_COLLECTION_NAME, nodeId).map(_._1).toList
        logger.info(s"Node $nodeId is responsible for partitions: $partitionsOwned")

        // Request partition ownership from other nodes
        for (ownerNodeId <- partitionsByOwnerToRequest.keys) {
            val partitions = partitionsByOwnerToRequest(ownerNodeId)
            logger.info(s"Node $nodeId is requesting ownership of partitions $partitions from node $ownerNodeId")
            requestPartitionOwnership(ownerNodeId, partitions)
        }

        // Setup consumers from job
        this.consumerPerPartition.clear()
        job.consumers.foreach: ref =>
            if (ref.chn != CHN_NEXMARK || partitionsOwned.contains(ref.partitions.head)) {
                val consumer = KafkaLogConsumer.fromRef(ref)
                logger.debug(s"Node $nodeId - Setting up consumer: $consumer for partition ${consumer.partition}")
                val partition = if (ref.chn == CHN_NEXMARK) consumer.partition else BROADCAST_PARTITION_ID
                this.consumerPerPartition.put(partition, (ref.chn, consumer))
            }

        // Setup consumers for other owned partitions
        for partitionId <- partitionsOwned do
            if !this.consumerPerPartition.contains(partitionId) then
                addNewNexmarkConsumer(partitionId)

        logger.info(s"Consumers: $consumerPerPartition")


        // Setup procFunctions
        this.partitions = partitionsOwned
        this.procFunctionPerPartition = scala.collection.mutable.Map(partitionsOwned.map { partition =>
            partition -> new RecordProcFun(partition)
        }: _*)

        // Recover from the last checkpoint for each partition and node
        this.checkpointManager.recoverCheckpoint(NODE_ID, partitionsOwned, procFunctionPerPartition, consumerPerPartition)
    }

    private def run(): Unit = {
        var time = 0L

        while true do
            val t = System.currentTimeMillis()

            // check the job queue every 1_000 milliseconds
            if (t - time) > 1_000 then
                time = t
                checkJobQueue()

            // Create checkpoint if its time for it
            this.checkpointManager.createCheckpointIfRequired(NODE_ID, procFunctionPerPartition, consumerPerPartition)


            // Check for failed nodes & handle failures
            val currentFailedNodes = failureDetector.checkNodeFailures()
            handleFailedNodes(currentFailedNodes.diff(failedNodes))
            this.failedNodes = currentFailedNodes

            runStep()
    }

    private inline def checkJobQueue(): Unit = {
        val job = this.queue.poll()
        if job != null then this.setup(job)
    }

    private inline def runStep(): Unit = {
        // 1. Poll, process each consumer
        for ((partitionId, (chn, consumer)) <- this.consumerPerPartition) {
            try {

                val records = consumer.poll()
                logger.debug(s"Node $nodeId is polling partition $partitionId from channel $chn: Consumer: $consumer - Records nonEmpty: ${records.nonEmpty}")

                if records.nonEmpty then {
                    chn match {
                        case CHN_BROADCAST =>
                            for (rec <- records) {
                                // Track heartbeats from other nodes
                                val (key, value) = rec
                                val message = readBinary[BroadcastMessage](value)

                                val senderId = message.senderId
                                message match {
                                    case CRDTUpdate(update, _) =>
                                        if (senderId != nodeId) {
                                            logger.info(s"Node $nodeId received CRDT update from node $senderId")
                                            failureDetector.setHeartbeat(senderId)
                                        }
                                        // Send broadcast to each processing function
                                        this.procFunctionPerPartition.foreach((_, procFun) =>
                                                                                  procFun.process(outputFunction, chn, Iterable.single((key, update)))
                                                                              )
                                    case OwnershipRequest(receiverId, partitions, senderId) =>
                                        if receiverId == NODE_ID then
                                            logger.info(s"($nodeId) Received ownership request from $senderId for partitions $partitions")
                                            handoverOwnership(senderId, partitions)

                                        // TODO delete: for now give node more time to recover
                                        if (senderId != nodeId) failureDetector.setHeartbeat(senderId, System.currentTimeMillis() + 5000)

                                    case OwnershipRequestAccepted(receiverId, partitions, senderId) =>
                                        if receiverId == NODE_ID then
                                            logger.info(s"(Node $NODE_ID) Received ownership confirmation from node $senderId for partitions $partitions")
                                            integrateNewPartitions(partitions)
                                }
                            }
                        case _ =>
                            // Send message for partition to specific processing function
                            val procFun = this.procFunctionPerPartition(partitionId)
                            if (procFun != null) {
                                logger.info(s"Node $nodeId is processing records for partition $partitionId")
                                procFun.process(outputFunction, chn, records)
                            }
                    }
                } else {
                    val kafkaConsumer = consumer.asInstanceOf[KafkaLogConsumer]
                    val partition = kafkaConsumer.partition
                    logger.debug(s"Node $nodeId - partition $partition received no records from channel $chn")
                }
            } catch {
                case e: IllegalStateException =>
                    logger.error(s"Error processing consumer for partition $partitionId", e)
            }
        }

        // 2. Flush all producers
        for ((chn, producer) <- producers) do producer.flush()
    }

    /**
     * Output function callback that send messages to the output channels.
     */
    def outputFunction(partitionId: Int, chn: Byte, recs: LogProducerRecords): Unit = {
        chn match {
            case Config.CHN_BROADCAST =>
                // Add id of current node to the broadcast message for heartbeat tracking
                val recordsWithNodeId = recs.map { case (key, value) =>
                    val message = CRDTUpdate(value, NODE_ID)
                    val serializedMessage = writeBinary(message)
                    (key, serializedMessage)
                }
                out.collect(chn, recordsWithNodeId)
            case Config.CHN_OUTPUT =>
                // Handle output for CHN_OUTPUT

                // Check if node is responsible for partition
                val (ownerNodeId, _) = FirestoreClient.queryNodeForPartition(FirestoreClient.OWNERSHIP_COLLECTION_NAME, partitionId)
                if ownerNodeId == nodeId then {
                    recs.foreach: r =>
                        val bids = readBinary[(Long)](r._2)
                        logger.info(s"Node $nodeId partition $partitionId commits: $bids")

                    out.collect(chn, recs)
                    this.failureDetector.setStartCheckingForFailures()
                } else {
                    logger.info(s"Node $nodeId cannot output because it is not responsible for partition $partitionId")
                }
            case _ =>
                logger.warn(s"Unknown channel: $chn")
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
            if (checkFailureRedistributionResponsibility(failedNode, failedNodes)) {
                logger.warn(s"Node $nodeId is responsible for redistribution of partitions from failed node $failedNode")
                val partitions = FirestoreClient.queryPartitionsByNodeId(FirestoreClient.OWNERSHIP_COLLECTION_NAME, failedNode)

                // Set new partition ownership for each partition
                for (partitionId, versionNr) <- partitions do
                    FirestoreClient.setPartitionOwnership(partitionId, nodeId, versionNr + 1)
                    logger.info(s"Node $nodeId is new owner of partition $partitionId")

                integrateNewPartitions(partitions.map(_._1))
            } else {
                logger.info(s"Node $nodeId is not responsible for redistribution of partitions from failed node $failedNode")
            }
    }

    /**
     * Integrate new partitions into the system. Create processing function, consumer & restore snapshot.
     */
    private def integrateNewPartitions(partitions: List[Int]): Unit = {
        for (partitionId <- partitions) {
            procFunctionPerPartition += partitionId -> new RecordProcFun(partitionId)
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
        logger.info(s"Node $nodeId - Adding new consumer for partition $partitionId: $inputConsumer")
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
     */
    private def requestPartitionOwnership(ownerNodeId: Int, partitions: List[Int]): Unit = {
        // Create the OwnershipRequest message
        val message = OwnershipRequest(ownerNodeId, partitions, NODE_ID)

        // Serialize the message
        val serializedMessage = writeBinary(message)

        // Send the serialized message
        val records = List((writeBinary(0), serializedMessage))
        out.collect(Config.CHN_BROADCAST, records)

        logger.info(s"Requesting ownership of partitions $partitions from node $ownerNodeId")
    }

    /**
     * Handover ownership of partitions to a new owner.
     * Checkpoint current state, set new owner & close consumer.
     * Send ownership confirmation to new owner.
     */
    private def handoverOwnership(newOwnerId: Int, partitions: List[Int]): Unit = {
        if partitions.nonEmpty then
            // Checkpoint current state, set new owner & close consumer
            var  transferredPartitions = List.empty[Int]
            for partitionId <- partitions do
                if (this.procFunctionPerPartition.keySet.contains(partitionId)) {

                    val (chn, consumer) = this.consumerPerPartition(partitionId)
                    val procFun = this.procFunctionPerPartition(partitionId)
                    this.checkpointManager.createCheckpointForPartition(partitionId, procFun, consumer)
                    FirestoreClient.setPartitionOwnership(partitionId, newOwnerId, 0)

                    consumer.close()
                    this.consumerPerPartition.remove(partitionId)
                    this.procFunctionPerPartition.remove(partitionId)
                    transferredPartitions = transferredPartitions :+ partitionId
                }

            logger.info(s"Partition $transferredPartitions ownership handed over to node $newOwnerId")

            // Send ownership confirmation to new owner
            val message = OwnershipRequestAccepted(newOwnerId, transferredPartitions, NODE_ID)
            val serializedMessage = writeBinary(message)
            val records = List((writeBinary(0), serializedMessage))
            out.collect(Config.CHN_BROADCAST, records)
    }

}
