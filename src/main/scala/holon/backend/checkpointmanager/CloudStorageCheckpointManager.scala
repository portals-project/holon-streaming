package holon.backend.checkpointmanager

import holon.*
import holon.backend.cloud.GCSClient.bucketName
import holon.backend.cloud.GCSClient
import holon.Config.CHN_CONTROL
import holon.messages.Checkpoint
import upickle.default.writeBinary

import scala.collection.immutable.Map

class CloudStorageCheckpointManager(outputCollector: OutputCollector) extends CheckpointManager {

    private val logger = Logger.apply("CloudStorageCheckpointManager")
    Logger.setLevel("CloudStorageCheckpointManager", "INFO")


    protected def savePartitionSnapshots(nodeId: Int, partitionSnapshots: scala.collection.mutable.Map[Int, (Long, String)]): Unit = {
        partitionSnapshots.foreach((partitionId, snapshotContent) => {
            val (offset, snapshot) = snapshotContent
            GCSClient.uploadStringToBucket(bucketName, getPartitionSnapshotName(partitionId), s"$offset:$snapshot")
        })
    }

    protected def saveNodeCheckpoint(nodeId: Int, nodeOffsets: String): Unit = {
        GCSClient.uploadStringToBucket(bucketName, getNodeSnapshotName(nodeId), nodeOffsets)
    }

    def saveSnapshotsFromOtherNodes(partitionSnapshots: Map[Int, (Long, String)]): Unit = {
        // Ignore snapshots from other nodes
    }

    /**
     * Recover checkpoint for all partitions and reset broadcast channel offset for node.
     */
    def recoverPartitionCheckpoints(nodeId: Int, partitionIds: List[Int],
        procFunctionPerPartition: scala.collection.mutable.Map[Int, ProcFun],
        consumerPerPartition: scala.collection.mutable.Map[Int, (Byte, LogConsumer)]): Unit = {

        // Restore snapshot for all partitions
        partitionIds.foreach(partitionId => {
            recoverCheckpointForPartition(partitionId, procFunctionPerPartition(partitionId), consumerPerPartition(partitionId)._2)
        })
    }

    def recoverNodeOffset(nodeId: Int, consumerPerPartition: scala.collection.mutable.Map[Int, (Byte, LogConsumer)]): Boolean = {
        val nodeFileExists = GCSClient.checkIfFileExists(bucketName, getNodeSnapshotName(nodeId))
        if (nodeFileExists) {
            val offset = GCSClient.downloadStringFromBucket(bucketName, getNodeSnapshotName(nodeId))
            logger.debug(s"Restoring broadcast & consumer channel offset for node $nodeId: $offset")
            setNodeConsumerOffsets(offset, consumerPerPartition)
        }
        nodeFileExists
    }

    def recoverCheckpointForPartition(partitionId: Int, procFun: ProcFun, consumer: LogConsumer): Unit = {
        if (GCSClient.checkIfFileExists(bucketName, getPartitionSnapshotName(partitionId))) {
            val fileContent = GCSClient.downloadStringFromBucket(bucketName, getPartitionSnapshotName(partitionId))
            val Array(offset: String, snapshotString: String) = fileContent.split(":")
            restorePartitionSnapshot(partitionId, offset.toLong, snapshotString, procFun, consumer)
        }
    }

    def sendCheckpointMessage(nodeId: Int): Unit = {
        // Not required for cloud storage implementation. As we are only sharing the offsets with other nodes
        // and in the cases where no partitionSnapshots are directly provided, we do not need to send a message.
    }

    def sendCheckpointMessage(nodeId: Int, partitionSnapshots: Map[Int, (Long, String)]): Unit = {
        // Only share offsets for each snapshot with other nodes. This is used to compare ownership when 2 nodes process the same partition.
        // The snapshot itself is not shared, as it is not needed for recovery.
        val sharableCheckpoints = scala.collection.mutable.Map[Int, (Long, String)]()
        for ((partitionId, snapshotContent) <- partitionSnapshots) {
            val (offset, _) = snapshotContent
            sharableCheckpoints.put(partitionId, (offset, ""))
        }

        val message = Checkpoint(nodeId, sharableCheckpoints.toMap)
        val serializedMessage = writeBinary(message)
        val records = List((writeBinary(0), serializedMessage))

        outputCollector.collect(CHN_CONTROL, records)
    }

    private def getPartitionSnapshotName(partitionId: Int): String = {
        "partition" + partitionId
    }

    private def getNodeSnapshotName(nodeId: Int): String = {
        "node" + nodeId
    }

}
