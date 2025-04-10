package holon.backend.checkpointmanager

import holon.example.nexmark.Config.*
import holon.*

import java.util.Base64
import scala.collection.immutable.Map

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
            val partitionSnapshots = scala.collection.mutable.Map[Int, (Long, String)]()
            procFunctionPerPartition.foreach((partitionId, procFun) => {
                val consumerOffset = consumerPerPartition(partitionId)._2.offsets().head._2
                val snapshotContent = createPartitionCheckpointContent(procFun)
                partitionSnapshots.put(partitionId, (consumerOffset,snapshotContent))
            })
            savePartitionSnapshots(nodeId, partitionSnapshots)
            sendCheckpointMessage(nodeId, partitionSnapshots.toMap)
            logger.info(s"Checkpoint created for partitions ${partitionSnapshots.keySet}")

            // Save broadcast & control channel offset for node.
            val (broadcastChn, broadcastConsumer) = consumerPerPartition(BROADCAST_PARTITION_ID)
            val (controlChn, controlConsumer) = consumerPerPartition(CONTROL_PARTITION_ID)
            val broadcastOffset = broadcastConsumer.offsets().head
            val controlOffset = controlConsumer.offsets().head

            saveNodeCheckpoint(nodeId, createNodeOffsetString(broadcastOffset._2, controlOffset._2))
            logger.debug(s"Node $nodeId saved broadcast & control channel offset: $broadcastOffset : $controlOffset")

            checkpointTime = System.currentTimeMillis()
        }
    }

    /**
     * Create checkpoint for a specific partition
     */
    def createCheckpointForPartition(nodeId: Int, partitionId: Int, procFun: ProcFun, consumer: LogConsumer): Unit = {
        val consumerOffset = consumer.offsets().head._2
        val checkpointContent = createPartitionCheckpointContent(procFun)
        val partitionSnapshots = scala.collection.mutable.Map[Int, (Long, String)]()
        partitionSnapshots.put(partitionId, (consumerOffset, checkpointContent))
        savePartitionSnapshots(nodeId, partitionSnapshots)
        logger.info(s"Checkpoint created for partition $partitionId")
    }

    /**
     * Save snapshots received from other nodes
     */
    def saveSnapshotsFromOtherNodes(partitionSnapshots: Map[Int, (Long, String)]): Unit

    /**
     * Create encoded snapshot string
     */
    private def createPartitionCheckpointContent(procFun: ProcFun) : String = {
        Base64.getEncoder.encodeToString(procFun.snapshot())
    }

    protected def savePartitionSnapshots(nodeId: Int, partitionSnapshots: scala.collection.mutable.Map[Int, (Long, String)]): Unit

    protected def saveNodeCheckpoint(nodeId: Int, nodeOffsets: String): Unit

    /** CHECKPOINT RECOVERY */

    def recoverPartitionCheckpoints(nodeId: Int, partitionIds: List[Int],
                                procFunctionPerPartition: scala.collection.mutable.Map[Int, ProcFun],
                                consumerPerPartition: scala.collection.mutable.Map[Int, (Byte, LogConsumer)]): Unit

    /**
     * Restore partition snapshot for processing function and consumer
     * Format: "offset:snapshot"
     */
    protected def restorePartitionSnapshot(partitionId: Int, offset: Long, snapshot: String, procFun: ProcFun, consumer: LogConsumer): Unit = {
        procFun.restore(Base64.getDecoder.decode(snapshot))
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

    def sendCheckpointMessage(nodeId: Int): Unit

    def sendCheckpointMessage(nodeId: Int, partitionSnapshots: Map[Int, (Long, String)]): Unit

    private def createNodeOffsetString(broadcastOffset: Long, controlOffset: Long): String = {
        broadcastOffset.toString + ":" + controlOffset.toString
    }
}
