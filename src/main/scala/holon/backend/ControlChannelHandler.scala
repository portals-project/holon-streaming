package holon.backend

import holon.*
import holon.backend.checkpointmanager.CheckpointManager
import holon.messages.*
import scala.collection.mutable

class ControlChannelHandler(nodeId: Int,
    controlState: ControlState,
    outputMsgHandler: OutputMessageHandler,
    failureDetector: FailureDetector,
    ownershipManager: PartitionOwnershipManager,
    checkpointManager: CheckpointManager) {

    private val logger = Logger.apply("ControlChannelHandler")
    Logger.setLevel("ControlChannelHandler", "INFO")

    def handle(msg: ControlMessage): Unit = {
        if (msg.senderId != nodeId) {
            logger.debug(s"Node $nodeId received control message: $msg")
            msg match {
                case Heartbeat(senderId) =>
                    failureDetector.recordHeartbeat(senderId)
                case Checkpoint(senderId, partitionSnapshots) =>
                    // Node Checkpoints are only send when checkpoints are not saved to cloud storage
                    this.checkpointManager.saveSnapshotsFromOtherNodes(partitionSnapshots)
                case OwnershipState(ownershipMap, senderId) =>
                    handleNewOwnershipState(ownershipMap)
                case OwnershipStateRequest(senderId) =>
                    // Send both checkpoints and ownership state to share latest state
                    this.checkpointManager.sendCheckpointMessage(nodeId)
                    val ownershipMap = ownershipManager.getOwnershipMap
                    outputMsgHandler.sendControlMessage(OwnershipState(ownershipMap, nodeId))
                case OwnershipTransferRequest(receiverId, partitions, senderId) =>
                    if (receiverId == nodeId) {
                        handleOwnershipTransferRequest(senderId, partitions)
                    }
                case OwnershipTransferDenial(receiverId, partitions, senderId) =>
                    if (receiverId == nodeId) {
                        logger.info(s"(Node $nodeId) Received ownership denial from node $senderId for partitions $partitions")
                        LagManager.updateLag(senderId, 0) // Reset lag for sender node
                    }
            }
        }
    }

    /**
     * Handle new ownership state received from other nodes.
     * Integrate new partitions into the system and drop partitions that are no longer owned.
     */
    private def handleNewOwnershipState(ownershipMap: mutable.Map[Int, OwnershipEntry]): Unit = {
        val newPartitions = ownershipManager.getNewOwnedPartitions(ownershipMap)
        controlState.integrateNewPartitions(newPartitions)

        var dropPartitions = ownershipManager.getPartitionsToRelease(ownershipMap)
        var partitionToKeep = -1
        // Keep one partition if all partitions are dropped
        if (dropPartitions.length > 0 && dropPartitions.length == controlState.procFunctionPerPartition.size) {
            logger.info(s"dropPartitions: $dropPartitions")
            partitionToKeep = dropPartitions.last
            // Remove the partition to keep from the list of partitions to drop
            dropPartitions = dropPartitions.filter(_ != partitionToKeep)
            logger.info(s"dropPartitions: $dropPartitions")

            logger.info(s"Node $nodeId cannot drop all partitions. Keeping partition: $partitionToKeep")
        }

        for (partitionId <- dropPartitions) {
            logger.info(s"Node $nodeId dropped partition $partitionId. Ownership: ${ownershipManager.getOwnershipMap}")
            controlState.dropPartition(partitionId)
        }
        ownershipManager.mergeOwnershipMap(ownershipMap)
        if (partitionToKeep != -1) {
            ownershipManager.setPartitionOwnership(partitionToKeep)
            // Update ownership state to keep 1 partition
            outputMsgHandler.sendControlMessage(OwnershipState(ownershipManager.getOwnershipMap, nodeId))
            logger.info(s"After keeping partition $partitionToKeep. Ownership: ${ownershipManager.getOwnershipMap}")
        }
    }

    /**
     * Handle ownership transfer request from other nodes.
     * If the partition list is empty, hand over the partition with largest lag. Else hand over the requested partitions.
     * Deny ownership transfer if all owned partitions are requested or there are no partitions with lag.
     */
    private def handleOwnershipTransferRequest(newOwnerId: Int, partitions: List[Int]): Unit = {
        logger.info(s"($nodeId) Received ownership request from $newOwnerId for partitions $partitions")
        var partitionsToHandover = partitions
        // If the partition list is empty (in case of work stealing), hand over partition with largest lag.
        if (partitionsToHandover.isEmpty) {
            val partitionIdAndLag = LagManager.getPartitionWithMaxLag(controlState.consumerPerPartition)
            if (partitionIdAndLag.isEmpty || partitionIdAndLag.get._2 <= 0) {
                logger.info(s"Node $nodeId cannot hand over any partitions to node $newOwnerId. No partitions with lag")
                outputMsgHandler.sendControlMessage(OwnershipTransferDenial(newOwnerId, partitions, nodeId))
                return
            }
            logger.info(s"Detected $partitionIdAndLag as partition with largest lag.")
            partitionsToHandover = List(partitionIdAndLag.get._1)
        }

        // Do not hand over last partition, otherwise this node becomes idle!
        val ownedPartitions = controlState.procFunctionPerPartition.keySet.toList
        if (partitionsToHandover.size == ownedPartitions.size) {
            logger.info(s"Node $nodeId cannot hand over all partitions to node $newOwnerId. At least one partition must remain")
            outputMsgHandler.sendControlMessage(OwnershipTransferDenial(newOwnerId, partitions, nodeId))
            return
        }

        handoverOwnership(newOwnerId, partitionsToHandover)
    }

    /**
     * Handover ownership of partitions to a new owner.
     * Checkpoint current state, set new owner & close consumer.
     * Send ownership confirmation to new owner.
     */
    private def handoverOwnership(newOwnerId: Int, partitions: List[Int]): Unit = {
        // Checkpoint current state, set new owner & close consumer
        var transferredPartitions = List.empty[Int]
        for (partitionId <- partitions) {
            if (controlState.procFunctionPerPartition.keySet.contains(partitionId)) {
                val (chn, consumer) = controlState.consumerPerPartition(partitionId)
                val procFun = controlState.procFunctionPerPartition(partitionId)
                this.checkpointManager.createCheckpointForPartition(nodeId, partitionId, procFun, consumer)
                ownershipManager.setPartitionOwnership(partitionId, newOwnerId)

                controlState.dropPartition(partitionId)
                transferredPartitions = transferredPartitions :+ partitionId
            }
        }

        // Send new checkpoint state
        this.checkpointManager.sendCheckpointMessage(nodeId)
        // Notify other nodes about updated ownership
        outputMsgHandler.sendControlMessage(messages.OwnershipState(ownershipManager.getOwnershipMap, nodeId))
        logger.info(s"Partition $transferredPartitions ownership handed over to node $newOwnerId")
        logger.info(s"Node $nodeId handed over ownership: ${ownershipManager.getOwnershipMap}")
    }

}
