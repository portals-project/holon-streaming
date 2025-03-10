package holon.backend

import holon.*
import holon.Utils.*
import holon.backend.GCSClient.bucketName
import holon.example.nexmark.Config.*
import upickle.default.*
import java.util.concurrent.ConcurrentLinkedQueue

class Recovery(nodeId: Int) {

    val NODE_ID: Int = nodeId

    private val BROADCAST_PARTITION_ID = -1
    private var partitions: List[Int] = List.empty
    private val consumerPerPartition = scala.collection.mutable.Map.empty[Int, (Byte, LogConsumer)]
    private val producers = scala.collection.mutable.Map.empty[Byte, LogProducer]
    private var procFunctionPerPartition: Map[Int, ProcFun] = Map.empty
    private val out = OutputCollectorImpl(producers)
    private val queue = new ConcurrentLinkedQueue[Job]()
    private val failureDetector = FailureDetector(nodeId)
    private val checkpointManager = CheckpointManager()
    private val logger = Logger.apply("Recovery")

    Logger.setLevel("Recovery", "INFO")

    RunThread(this.run())

    def submitOrUpdate(job: Job): Unit = {
        this.queue.add(job)
    }

    private def setup(job: Job): Unit = {
        val partitions = job.partitions

        logger.info(s"Setting up job for node $nodeId")
        // Setup consumers
        this.consumerPerPartition.clear()
        job.consumers.foreach: ref =>
            val consumer = KafkaLogConsumer.fromRef(ref)
            logger.debug(s"Node $nodeId - Setting up consumer: $consumer for partition ${consumer.partition}")
            val partition = if (ref.chn == CHN_NEXMARK) consumer.partition else BROADCAST_PARTITION_ID
            this.consumerPerPartition.put(partition, (ref.chn, consumer))

        logger.debug(s"Consumers: $consumerPerPartition")


        for partitionId <- partitions do
            // Check partition ownership
            val (ownerNodeId, versionNr) = FirestoreClient.queryNodeForPartition(FirestoreClient.OWNERSHIP_COLLECTION_NAME, partitionId)

            // If partition is not owned by any node, set ownership to current node
            if ownerNodeId == -1 then
                FirestoreClient.setPartitionOwnership(partitionId, nodeId, 0)
            else if ownerNodeId != nodeId then
            // Ask for ownership of partition


                logger.info(s"Node $nodeId is not responsible for partition $partitionId")
            else
                logger.info(s"Node $nodeId is already responsible for partition $partitionId")

        // Setup producers
        this.producers.clear()
        job.producers.foreach: ref =>
            val producer = KafkaLogProducer.fromRef(ref)
            this.producers.put(ref.chn, producer)

        // Setup procFunctions
        this.partitions = job.partitions
        this.procFunctionPerPartition = job.partitions.map { partition =>
            partition -> new RecordProcFun(partition)
        }.toMap

        // Set partition ownership
        this.partitions.foreach(partitionId => {
            FirestoreClient.setPartitionOwnership(partitionId, nodeId, 0)
        })

        // Recover from the last checkpoint for each partition
        this.checkpointManager.recoverCheckpointForAllPartitions(this.partitions, true,
                                                                 this.procFunctionPerPartition, this.consumerPerPartition)
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
            this.checkpointManager.createCheckpointIfRequired(procFunctionPerPartition, consumerPerPartition)


            // Check for failed nodes & handle failures
            handleFailedNodes(failureDetector.checkNodeFailures());

            runStep()
    }

    private inline def checkJobQueue(): Unit = {
        val job = this.queue.poll()
        if job != null then this.setup(job)
    }

    private inline def runStep(): Unit = {
        // 1. Poll, process each consumer
        for ((partitionId, (chn, consumer)) <- consumerPerPartition) {
            val records = consumer.poll()
            logger.debug(s"Node $nodeId is polling partition $partitionId from channel $chn: Consumer: $consumer - Records nonEmpty: ${records.nonEmpty}")

            if records.nonEmpty then {
                chn match {
                    case CHN_BROADCAST =>
                        for (rec <- records) {
                            // Track heartbeats from other nodes
                            val (key, value) = rec
                            val (receivedNodeId, recValue) = readBinary[(Int, Array[Byte])](value)
                            logger.debug(s"($nodeId) Received broadcast from $receivedNodeId")

                            if (receivedNodeId != nodeId) failureDetector.setHeartbeat(receivedNodeId)

                            // Send broadcast to each processing function
                            this.procFunctionPerPartition.foreach((_, procFun) =>
                                procFun.process(outputFunction, chn, Iterable.single((key, recValue)))
                            )
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
                if nodeId == 0 then return // Skip broadcasting from node 0

                // Add id of current node to the broadcast message for heartbeat tracking
                val recordsWithNodeId = recs.map { case (key, value) =>
                    (key, writeBinary((nodeId, value)))
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

                // Create procFunction, consumer & restore snapshot for each partition
                for ((partitionId, versionNr) <- partitions) {
                    procFunctionPerPartition += partitionId -> new RecordProcFun(partitionId)
                    val inputConsumer = KafkaLogConsumer.fromRef(ConsumerRef(
                        chn = CHN_NEXMARK,
                        host = KAFKA_HOST,
                        port = KAFKA_PORT,
                        topic = KAFKA_TOPIC_NEXMARK,
                        partitions = List(partitionId),
                        ))
                    this.consumerPerPartition.put(partitionId, (CHN_NEXMARK, inputConsumer))

                    this.checkpointManager.recoverCheckpoint(partitionId, false,
                                                             procFunctionPerPartition, consumerPerPartition)
                }
            } else {
                logger.info(s"Node $nodeId is not responsible for redistribution of partitions from failed node $failedNode")
            }
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

}
