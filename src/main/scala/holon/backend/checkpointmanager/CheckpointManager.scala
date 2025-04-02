package holon.backend.checkpointmanager

import holon.example.nexmark.Config.*
import holon.*

import java.util.Base64

abstract class CheckpointManager {

    private var checkpointTime = System.currentTimeMillis()

    private val logger = Logger.apply("CheckpointManager")
    Logger.setLevel("CheckpointManager", "INFO")

    /** CHECKPOINT CREATION */

    /**
     * Checks if its time to create a checkpoint.
     * If so, creates a checkpoint for each partition.
     */
    def createCheckpointIfRequired(nodeId: Int, procFunctionPerPartition: scala.collection.mutable.Map[Int, ProcFun],
        consumerPerPartition: scala.collection.mutable.Map[Int, (Byte, LogConsumer)]): Unit = {

        val t = System.currentTimeMillis()
        if (t - checkpointTime > CHECKPOINT_INTERVAL) {
            // Save snapshot for all partitions
            val partitionSnapshots = scala.collection.mutable.Map[Int, String]()
            procFunctionPerPartition.foreach((partitionId, procFun) => {
                val snapshotContent = createPartitionCheckpointContent(procFun, consumerPerPartition(partitionId)._2)
                partitionSnapshots.put(partitionId, snapshotContent)
            })
            safePartitionSnapshots(nodeId, partitionSnapshots)
            logger.info(s"Checkpoint created for partitions ${partitionSnapshots.keySet}")

            // Save broadcast & control channel offset for node.
            val (broadcastChn, broadcastConsumer) = consumerPerPartition(BROADCAST_PARTITION_ID)
            val (controlChn, controlConsumer) = consumerPerPartition(CONTROL_PARTITION_ID)
            val broadcastOffset = broadcastConsumer.offsets().head
            val controlOffset = controlConsumer.offsets().head

            safeNodeCheckpoint(nodeId, createNodeOffsetString(broadcastOffset._2, controlOffset._2))
            logger.debug(s"Node $nodeId saved broadcast & control channel offset: $broadcastOffset : $controlOffset")

            checkpointTime = System.currentTimeMillis()
        }
    }

    /**
     * Create checkpoint for a specific partition
     */
    def createCheckpointForPartition(nodeId: Int, partitionId: Int, procFun: ProcFun, consumer: LogConsumer): Unit = {
        val checkpointContent = createPartitionCheckpointContent(procFun, consumer)
        val partitionSnapshots = scala.collection.mutable.Map[Int, String]()
        partitionSnapshots.put(partitionId, checkpointContent)
        safePartitionSnapshots(nodeId, partitionSnapshots)
        logger.info(s"Checkpoint created for partition $partitionId")
    }

    /**
     * Create checkpoint content for a partition
     * Format: "consumerOffset:snapshot"
     */
    private def createPartitionCheckpointContent(procFun: ProcFun, consumer: LogConsumer) : String = {
        val partitionOffset = consumer.offsets().head
        val base64EncodedSnapshot = Base64.getEncoder.encodeToString(procFun.snapshot())
        s"${partitionOffset._2}:$base64EncodedSnapshot"
    }

    protected def safePartitionSnapshots(nodeId: Int, partitionSnapshots: scala.collection.mutable.Map[Int, String]): Unit

    protected def safeNodeCheckpoint(nodeId: Int, nodeOffsets: String): Unit

    /** CHECKPOINT RECOVERY */

    def recoverPartitionCheckpoints(nodeId: Int, partitionIds: List[Int],
                                procFunctionPerPartition: scala.collection.mutable.Map[Int, ProcFun],
                                consumerPerPartition: scala.collection.mutable.Map[Int, (Byte, LogConsumer)]): Unit

    /**
     * Restore partition snapshot for processing function and consumer
     * Format: "offset:snapshot"
     */
    protected def restorePartitionSnapshot(partitionId: Int, snapshotContent: String, procFun: ProcFun, consumer: LogConsumer): Unit = {
        val Array(offset: String, snapshotString: String) = snapshotContent.split(":")
        procFun.restore(Base64.getDecoder.decode(snapshotString))
        consumer.seek(partitionId, offset.toLong)
        logger.debug(s"Restored snapshot for partition $partitionId.")
    }

    /**
     * Recover checkpoint for a specific partition
     */
    def recoverCheckpointForPartition(partitionId: Int, procFun: ProcFun, consumer: LogConsumer): Unit


    /**
     * Restore broadcast & control channel offset for node
     * @return true if node file exisits
     */
    def recoverNodeOffset(nodeId: Int, consumerPerPartition: scala.collection.mutable.Map[Int, (Byte, LogConsumer)]): Boolean

    /**
     * Set broadcast & control channel offset for node
     */
    protected def setNodeConsumerOffsets(offsetString: String, consumerPerPartition: scala.collection.mutable.Map[Int, (Byte, LogConsumer)]): Unit = {
        val Array(broadcastOffset, controlOffset) = offsetString.split(":")
        val (_, broadcastConsumer) = consumerPerPartition(BROADCAST_PARTITION_ID)
        val (_, controlConsumer) = consumerPerPartition(CONTROL_PARTITION_ID)
        broadcastConsumer.seek(0, broadcastOffset.toLong)
        controlConsumer.seek(0, controlOffset.toLong)
    }

    /** HELPERS */

    private def createNodeOffsetString(broadcastOffset: Long, controlOffset: Long): String = {
        broadcastOffset.toString + ":" + controlOffset.toString
    }
}
