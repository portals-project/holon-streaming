package holon.backend.checkpointmanager

import holon.backend.GCSClient
import holon.backend.GCSClient.bucketName
import holon.backend.checkpointmanager.CheckpointManager
import holon.example.nexmark.Config.*
import holon.*

class CloudStorageCheckpointManager extends CheckpointManager {

    private val logger = Logger.apply("CloudStorageCheckpointManager")
    Logger.setLevel("CloudStorageCheckpointManager", "INFO")


    protected def safePartitionSnapshots(nodeId: Int, partitionSnapshots: scala.collection.mutable.Map[Int, String]): Unit = {
        partitionSnapshots.foreach((partitionId, snapshot) => {
            GCSClient.uploadStringToBucket(bucketName, getPartitionSnapshotName(partitionId), snapshot)
        })
    }

    protected def safeNodeCheckpoint(nodeId: Int, nodeOffsets: String): Unit = {
        GCSClient.uploadStringToBucket(bucketName, getNodeSnapshotName(nodeId), nodeOffsets)
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
            restorePartitionSnapshot(partitionId, fileContent, procFun, consumer)
        }
    }

    private def getPartitionSnapshotName(partitionId: Int): String = {
        "partition" + partitionId
    }

    private def getNodeSnapshotName(nodeId: Int): String = {
        "node" + nodeId
    }

}
