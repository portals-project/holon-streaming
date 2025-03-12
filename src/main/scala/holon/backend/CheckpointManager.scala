package holon.backend

import holon.*
import holon.backend.GCSClient.bucketName
import holon.example.CRDT.crdtFromBinaryWithManifest
import org.apache.pekko.cluster.ddata.GCounter
import holon.{LogConsumer, ProcFun}

import java.util.Base64

class CheckpointManager {

    private val CHECKPOINT_INTERVAL = 5_000L
    private val BROADCAST_PARTITION_ID = -1

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
                logger.debug(s"Checkpoint created for partition $partitionId: ${crdtFromBinaryWithManifest(snapshot)._2.asInstanceOf[GCounter]}")
            })

            // Save broadcast channel offset for node
            val (broadcastChn, broadcastConsumer) = consumerPerPartition(BROADCAST_PARTITION_ID)
            val broadcastOffset = broadcastConsumer.offsets().head
            logger.debug(s"Node $nodeId saving broadcast channel offset: $broadcastOffset")
            GCSClient.uploadStringToBucket(bucketName, getNodeSnapshotName(nodeId), broadcastOffset._2.toString)


            checkpointTime = System.currentTimeMillis()
        }
    }

    /**
     * Create checkpoint for a specific partition
     */
    def createCheckpointForPartition(partitionId: Int, procFun: ProcFun, consumer: LogConsumer): Unit = {
        val snapshot = procFun.snapshot()
        safePartitionSnapshot(partitionId, snapshot, consumer)
        logger.debug(s"Checkpoint created for partition $partitionId: ${crdtFromBinaryWithManifest(snapshot)._2.asInstanceOf[GCounter]}")
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

        // Restore broadcast channel offset for node
        if (GCSClient.checkIfFileExists(bucketName, getNodeSnapshotName(nodeId))) {
            val offset = GCSClient.downloadStringFromBucket(bucketName, getNodeSnapshotName(nodeId))
            logger.debug(s"Restoring broadcast channel offset for node $nodeId: $offset")
            val (broadcastChn, broadcastConsumer) = consumerPerPartition(BROADCAST_PARTITION_ID)
            broadcastConsumer.seek(0, offset.toLong)
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

        logger.debug(s"Restored snapshot for partition $partitionId: $fileContent")
    }

    private def getPartitionSnapshotName(partitionId: Int): String = {
        "partition" + partitionId
    }

    private def getNodeSnapshotName(nodeId: Int): String = {
        "node" + nodeId
    }

}
