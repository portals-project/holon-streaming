package holon.backend

import holon.*
import holon.example.nexmark.Config.*
import holon.backend.GCSClient.bucketName
import holon.example.CRDT.crdtFromBinaryWithManifest
import org.apache.pekko.cluster.ddata.GCounter
import holon.{LogConsumer, ProcFun}

import java.util.Base64

class CheckpointManager {

    private var checkpointTime = System.currentTimeMillis()
    private val logger = Logger.apply("CheckpointManager")

    Logger.setLevel("CheckpointManager", "INFO")

    /**
     * Checks if its time to create a checkpoint.
     * If so, creates a checkpoint for each partition.
     */
    def createCheckpointIfRequired(nodeId: Int, procFunctionPerPartition: scala.collection.mutable.Map[Int, ProcFun],
        consumerPerPartition: scala.collection.mutable.Map[Int, (Byte, LogConsumer)]): Unit = {

        val t = System.currentTimeMillis()
        if (t - checkpointTime > CHECKPOINT_INTERVAL) {
            // Save snapshot for all partitions
            procFunctionPerPartition.foreach((partitionId, procFun) => {
                val snapshot = procFun.snapshot()
                safePartitionSnapshot(partitionId, snapshot, consumerPerPartition(partitionId)._2)
                logger.info(s"Checkpoint created for partition $partitionId")
            })

            // Save broadcast & control channel offset for node.
            val (broadcastChn, broadcastConsumer) = consumerPerPartition(BROADCAST_PARTITION_ID)
            val (controlChn, controlConsumer) = consumerPerPartition(CONTROL_PARTITION_ID)
            val broadcastOffset = broadcastConsumer.offsets().head
            val controlOffset = controlConsumer.offsets().head

            logger.debug(s"Node $nodeId saving broadcast & control channel offset: $broadcastOffset : $controlOffset")
            val offset = createNodeOffsetString(broadcastOffset._2, controlOffset._2)
            GCSClient.uploadStringToBucket(bucketName, getNodeSnapshotName(nodeId), offset)


            checkpointTime = System.currentTimeMillis()
        }
    }

    /**
     * Create checkpoint for a specific partition
     */
    def createCheckpointForPartition(partitionId: Int, procFun: ProcFun, consumer: LogConsumer): Unit = {
        val snapshot = procFun.snapshot()
        safePartitionSnapshot(partitionId, snapshot, consumer)
        logger.info(s"Checkpoint created for partition $partitionId")
    }

    /**
     * Safe partition snapshot of current state to Google Cloud Storage
     * Saves offset for partition consumer & current state
     * Format: "offset:snapshot"
     */
    private def safePartitionSnapshot(partitionId: Int, snapshot: Array[Byte], consumer: LogConsumer): Unit = {
        val partitionOffset = consumer.offsets().head
        logger.debug(s"Partition $partitionId offset: $partitionOffset")

        val base64EncodedSnapshot = Base64.getEncoder.encodeToString(snapshot)
        val content = s"${partitionOffset._2}:$base64EncodedSnapshot"

        GCSClient.uploadStringToBucket(bucketName, getPartitionSnapshotName(partitionId), content)
    }


    /**
     * Recover checkpoint for all partitions and reset broadcast channel offset for node.
     */
    def recoverCheckpoint(nodeId: Int, partitionIds: List[Int],
        procFunctionPerPartition: scala.collection.mutable.Map[Int, ProcFun],
        consumerPerPartition: scala.collection.mutable.Map[Int, (Byte, LogConsumer)]) : Unit = {

        // Restore snapshot for all partitions
        partitionIds.foreach(partitionId => {
            if (GCSClient.checkIfFileExists(bucketName, getPartitionSnapshotName(partitionId))) {
                logger.debug(s"Restoring snapshot for partition $partitionId")
                restorePartitionSnapshot(partitionId, procFunctionPerPartition(partitionId), consumerPerPartition(partitionId)._2)
            }
        })

        // Restore broadcast & control channel offset for node
        if (GCSClient.checkIfFileExists(bucketName, getNodeSnapshotName(nodeId))) {
            val offset = GCSClient.downloadStringFromBucket(bucketName, getNodeSnapshotName(nodeId))
            logger.debug(s"Restoring broadcast & consumer channel offset for node $nodeId: $offset")
            val Array(broadcastOffset, controlOffset) = offset.split(":")
            val (_, broadcastConsumer) = consumerPerPartition(BROADCAST_PARTITION_ID)
            val (_, controlConsumer) = consumerPerPartition(CONTROL_PARTITION_ID)
            broadcastConsumer.seek(0, broadcastOffset.toLong)
            controlConsumer.seek(0, controlOffset.toLong)
        }

    }

    /**
     * Recover checkpoint for a specific partition
     */
    def recoverCheckpointForPartition(partitionId: Int, procFun: ProcFun, consumer: LogConsumer): Unit = {
        if (GCSClient.checkIfFileExists(bucketName, getPartitionSnapshotName(partitionId))) {
            logger.debug(s"Restoring snapshot for partition $partitionId")
            restorePartitionSnapshot(partitionId, procFun, consumer)
        }
    }

    /**
     * Restore partition snapshot from Google Cloud Storage
     * Format: "offset:snapshot"
     */
    private def restorePartitionSnapshot(partitionId: Int, procFun: ProcFun, consumer: LogConsumer): Unit = {
        val fileContent = GCSClient.downloadStringFromBucket(bucketName, getPartitionSnapshotName(partitionId))
        // Split the content on :
        val Array(offset: String, snapshotString: String) = fileContent.split(":")

        procFun.restore(Base64.getDecoder.decode(snapshotString))
        consumer.seek(partitionId, offset.toLong)

        logger.debug(s"Restored snapshot for partition $partitionId.")
    }

    private def getPartitionSnapshotName(partitionId: Int): String = {
        "partition" + partitionId
    }

    private def getNodeSnapshotName(nodeId: Int): String = {
        "node" + nodeId
    }

    private def createNodeOffsetString(broadcastOffset: Long, controlOffset: Long): String = {
        broadcastOffset.toString + ":" + controlOffset.toString
    }

}
