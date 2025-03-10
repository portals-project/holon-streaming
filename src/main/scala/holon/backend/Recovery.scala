package holon.backend

import holon.*
import holon.Utils.*
import holon.backend.GCSClient.bucketName
import holon.example.CRDT.crdtFromBinaryWithManifest
import holon.example.nexmark.Config.*
import org.apache.pekko.cluster.ddata.GCounter
import upickle.default.*

import java.util.Base64
import java.util.concurrent.ConcurrentLinkedQueue

class Recovery(nodeId: Int) {

    val NODE_ID: Int = nodeId

    private val BROADCAST_PARTITION_ID = -1
    private var partitions: List[Int] = List.empty
    private val consumers = scala.collection.mutable.Map.empty[Int, (Byte, LogConsumer)]
    private val producers = scala.collection.mutable.Map.empty[Byte, LogProducer]
    private var procFunctionPerPartition: Map[Int, ProcFun] = Map.empty
    private val out = OutputCollectorImpl(producers)
    private val queue = new ConcurrentLinkedQueue[Job]()
    private val failureDetector = FailureDetector(nodeId)
    private val logger = Logger.apply("Recovery")

    private val hearbeatMap = scala.collection.mutable.Map.empty[Int, Long]

    Logger.setLevel("Recovery", "INFO")

    // Checkpoint interval in milliseconds
    private val CHECKPOINT_INTERVAL = 5_000L

    RunThread(this.run())

    def submitOrUpdate(job: Job): Unit = {
        this.queue.add(job)
    }

    private def setup(job: Job): Unit = {
        val partitions = job.partitions

        logger.info(s"Setting up job for node $nodeId")
        // Setup consumers
        this.consumers.clear()
        job.consumers.foreach: ref =>
            val consumer = KafkaLogConsumer.fromRef(ref)
            logger.debug(s"Node $nodeId - Setting up consumer: $consumer for partition ${consumer.partition}")
            val partition = if (ref.chn == CHN_NEXMARK) consumer.partition else BROADCAST_PARTITION_ID
            this.consumers.put(partition, (ref.chn, consumer))

        logger.debug(s"Consumers: $consumers")


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

        // Recover from the last snapshot for each partition
        this.partitions.foreach(partitionId => {
            if (GCSClient.checkIfFileExists(bucketName, getSnapshotName(partitionId))) {
                logger.debug(s"Restoring snapshot for partition $partitionId")
                restoreSnapshot(getSnapshotName(partitionId), partitionId, true)
            }
        })
    }

    private def run(): Unit = {
        var time = 0L
        var checkpointTime = System.currentTimeMillis()

        failureDetector.initializeHeartBeatMap(NODE_ID);

        while true do
            val t = System.currentTimeMillis()

            // check the job queue every 1_000 milliseconds
            if (t - time) > 1_000 then
                time = t
                checkJobQueue()

            // Take a snapshot after every checkpoint interval
            if (t - checkpointTime) > CHECKPOINT_INTERVAL then
                this.procFunctionPerPartition.foreach((partitionId, procFun) => {
                    logger.info(s"Checkpointing partition $partitionId at ${System.currentTimeMillis()}")
                    val (partitionChn, partitionConsumer) = this.consumers(partitionId)
                    logger.info(s"Node $nodeId partition $partitionId offset ${partitionConsumer.offsets().head}")
                    val snapshot = procFun.snapshot()
                    logger.info(s"Node $nodeId partition $partitionId snapshot before ${crdtFromBinaryWithManifest(snapshot)._2.asInstanceOf[GCounter]}")
                    safeSnapshot(getSnapshotName(partitionId), partitionId, snapshot)
                    logger.info(s"Node $nodeId partition $partitionId snapshot after ${crdtFromBinaryWithManifest(snapshot)._2.asInstanceOf[GCounter]}")
                })
                checkpointTime = System.currentTimeMillis()
                logger.info(s"Checkpointed done for node $nodeId at ${System.currentTimeMillis()}")


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
        for ((partitionId, (chn, consumer)) <- consumers) {
            val records = consumer.poll()
            logger.debug(s"Node $nodeId is polling partition $partitionId from channel $chn: Consumer: $consumer - Records nonEmpty: ${records.nonEmpty}")

            if records.nonEmpty then {
                chn match {
                    case CHN_BROADCAST =>
                        // Update the heartbeat map for each received broadcast
                        val currentTime = System.currentTimeMillis()

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
                // Add id of current node to the broadcast message for heartbeat tracking
                val recordsWithNodeId = recs.map { case (key, value) =>
                    (key, writeBinary((nodeId, value)))
                }
                out.collect(chn, recordsWithNodeId)
            case Config.CHN_OUTPUT =>
                // Handle output for CHN_OUTPUT

                // Check if node is responsible for partition before committing
                val (ownerNodeId, _) =
                    if ownerNodeId == nodeId then {
                        recs.foreach: r =>
                            val bids = readBinary[(Long)](r._2)
                            logger.info(s"Node $nodeId partition $partitionId commits: $bids")

                        // Committing output
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
        logger.warn(s"Node $nodeId detected other failed nodes: $failedNodes")

        for failedNode <- failedNodes do
            // Check if current node needs to take over partitions from failed node
            if checkFailureRedistributionResponsibility(failedNode, failedNodes) then
                logger.warn(s"Node $nodeId is responsible for redistribution of partitions from failed node $failedNode")
                val partitions = FirestoreClient.queryPartitionsByNodeId(FirestoreClient.OWNERSHIP_COLLECTION_NAME, failedNode)

                // Set new partition ownership for each partition
                for (partitionId, versionNr) <- partitions do
                    FirestoreClient.setPartitionOwnership(partitionId, nodeId, versionNr + 1)
                    logger.info(s"Node $nodeId is new owner of partition $partitionId")

                // Create procFunction, consumer & restore snapshot for each partition
                for (partitionId, versionNr) <- partitions do
                    procFunctionPerPartition += partitionId -> new RecordProcFun(partitionId)
                    val inputConsumer = KafkaLogConsumer.fromRef(ConsumerRef(
                        chn = CHN_NEXMARK,
                        host = KAFKA_HOST,
                        port = KAFKA_PORT,
                        topic = KAFKA_TOPIC_NEXMARK,
                        partitions = List(partitionId),
                        ))
                    this.consumers.put(partitionId, (CHN_NEXMARK, inputConsumer))

                    val snapshotName = getSnapshotName(partitionId)
                    if GCSClient.checkIfFileExists(bucketName, snapshotName) then
                        restoreSnapshot(snapshotName, partitionId, false)

                    else
                        logger.info(s"Node $nodeId is not responsible for redistribution of partitions from failed node $failedNode")
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
     * Safe snapshot of current state to Google Cloud Storage
     * Saves offset for partition consumer and broadcast consumer
     * Format: {channel: [partition,offset]}snapshot
     * E.g. {0:[1,1110];1:[0,4]}snapshot
     */
    private def safeSnapshot(snapshotName: String, partitionId: Int, snapshot: Array[Byte]): Unit = {
        val (partitionChn, partitionConsumer) = this.consumers(partitionId)
        val partitionOffset = partitionConsumer.offsets().head
        logger.info(s"Node $nodeId partition $partitionId offset: $partitionOffset")

        val (broadcastChn, broadcastConsumer) = this.consumers(BROADCAST_PARTITION_ID)
        val broadcastOffset = broadcastConsumer.offsets().head

        val base64EncodedSnapshot = Base64.getEncoder.encodeToString(snapshot)
        val content = s"{$partitionChn:[$partitionId,${partitionOffset._2}];$broadcastChn:[0,${broadcastOffset._2}]}$base64EncodedSnapshot"

        logger.debug(s"Snapshot for partition $partitionId: $content")

        // Get GCSUploader object
        GCSClient.uploadStringToBucket(bucketName, snapshotName, content)
    }

    /**
     * Load snapshot from Google Cloud Storage and restore state.
     * Set offset for both partition and broadcast consumer.
     */
    private def restoreSnapshot(snapshotName: String, partitionId: Int, restoreBroadcastChannel: Boolean): Unit = {
        val snapshotString = GCSClient.downloadStringFromBucket(bucketName, snapshotName)

        // Get string after closing }
        val snapshotBase64Encoded = snapshotString.split("}")(1)

        // Get content between { } and split on ;
        val contentBetweenBraces = snapshotString.substring(snapshotString.indexOf("{") + 1, snapshotString.indexOf("}"))

        // Split the content on ;
        val offsetsPerChannel = contentBetweenBraces.split(";").map { pair =>
            val Array(key, value) = pair.split(":")
            val values = value.stripPrefix("[").stripSuffix("]").split(",").map(_.toInt)
            key.toInt -> values
        }.toMap

        // Restore the state from the snapshot
        val procFun = this.procFunctionPerPartition.get(partitionId)
        if (procFun.nonEmpty) {
            procFun.get.restore(Base64.getDecoder.decode(snapshotBase64Encoded))
            logger.debug(s"Restored snapshot for Node $nodeId: $snapshotString")
        }

        val (partitionChn, partitionConsumer) = this.consumers(partitionId)
        partitionConsumer.seek(offsetsPerChannel(partitionChn)(0), offsetsPerChannel(partitionChn)(1))

        if restoreBroadcastChannel then
            val (broadcastChn, broadcastConsumer) = this.consumers(BROADCAST_PARTITION_ID)
            broadcastConsumer.seek(offsetsPerChannel(broadcastChn)(0), offsetsPerChannel(broadcastChn)(1))
    }

    private def getSnapshotName(partitionId: Int): String = {
        "partition" + partitionId
    }

}
