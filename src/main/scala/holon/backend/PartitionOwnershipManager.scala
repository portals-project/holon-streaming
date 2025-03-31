package holon.backend

import holon.*
import holon.example.nexmark.Config.N_NODES

class PartitionOwnershipManager(nodeId: Int) {
    var partitionToNodeId: scala.collection.mutable.Map[Int, OwnershipEntry] = scala.collection.mutable.Map()
    var controlChannelConsumer: Option[LogConsumer] = None
    private val logger = Logger.apply("PartitionOwnershipManager")

    Logger.setLevel("PartitionOwnershipManager", "INFO")

    def setControlChannelConsumer(controlChannelConsumer: LogConsumer): Unit = {
        this.controlChannelConsumer = Some(controlChannelConsumer)
    }

    def initializePartitionOwnership(partitions: List[Int]): Unit = {
        for (partition <- partitions) {
            // Initialize with 0 so it does not overwrite existing ownership (in the case that node failed an rejoined)
            val ownershipEntry = OwnershipEntry(nodeId, 0)
            partitionToNodeId.put(partition, ownershipEntry)
        }
    }

    def setPartitionOwnership(partition: Int): Unit = {
        setPartitionOwnership(partition, nodeId)
    }

    def setPartitionOwnership(partition: Int, newOwnerId: Int): Unit = {
        val controlChannelOffset = controlChannelConsumer.get.offsets().head._2
        val ownershipEntry = OwnershipEntry(newOwnerId, controlChannelOffset)
        partitionToNodeId.put(partition, ownershipEntry)
    }

    def getPartitionOwner(partition: Int): Int = {
        partitionToNodeId.get(partition) match {
            case Some(ownershipEntry) => ownershipEntry.nodeId
            case None => -1
        }
    }

    def getPartitionsOwnedByNode(nodeId: Int): List[Int] = {
        //partitionToNodeId.entries.filter(_._2.nodeId == nodeId).map(_._1).toList
        partitionToNodeId.filter(_._2.nodeId == nodeId).keys.toList
    }

    /**
     * Merge the ownership map of the failed node with the current ownership map.
     * If the version of the ownership entry is higher than the current ownership entry, then update the ownership entry.
     * If the version is the same, then keep the ownership entry with the lower node id.
     * Otherwise, keep the current ownership entry.
     */
    def mergeOwnershipMap(other: scala.collection.mutable.Map[Int, OwnershipEntry]): Unit = {
        for ((partition, ownershipEntry) <- other) {
            partitionToNodeId.get(partition) match {
                case Some(currentOwnershipEntry) =>
                    if (ownershipEntry.version > currentOwnershipEntry.version) {
                        partitionToNodeId.put(partition, ownershipEntry)
                    } else if (ownershipEntry.version == currentOwnershipEntry.version && ownershipEntry.nodeId < currentOwnershipEntry.nodeId) {
                        partitionToNodeId.put(partition, ownershipEntry)
                    }
                case None =>
                    partitionToNodeId.put(partition, ownershipEntry)
            }
        }
        logger.info(s"Merged ownership map: $partitionToNodeId")
    }

    def getOwnershipMap: scala.collection.mutable.Map[Int, OwnershipEntry] = {
        partitionToNodeId
    }

    /**
     * Check if the current node is responsible for taking over the partitions of the failed node.
     */
    def checkOwnershipResponsibilityAfterFailure(failedNode: Int, failedNodes: List[Int]): Boolean = {
        var owner = failedNode
        while failedNodes.contains(owner) do
            // TODO add test for this to be sure!
            // If the failed node is also the current node, then the current node is responsible for redistribution
            owner = (owner + 1) % N_NODES

        owner == nodeId
    }

}
