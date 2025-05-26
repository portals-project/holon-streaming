package holon.backend

import holon.*
import Config.N_NODES
import holon.messages.OwnershipEntry

class PartitionOwnershipManager(nodeId: Int) {
    private val partitionToOwner: scala.collection.mutable.Map[Int, OwnershipEntry] = scala.collection.mutable.Map()
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
            partitionToOwner.put(partition, ownershipEntry)
        }
    }

    def setPartitionOwnership(partition: Int): Unit = {
        setPartitionOwnership(partition, nodeId)
    }

    def setPartitionOwnership(partition: Int, newOwnerId: Int): Unit = {
        val controlChannelOffset = controlChannelConsumer.get.offsets().head._2
        val ownershipEntry = OwnershipEntry(newOwnerId, controlChannelOffset)
        partitionToOwner.put(partition, ownershipEntry)
    }

    def getPartitionOwner(partition: Int): Int = {
        partitionToOwner.get(partition) match {
            case Some(ownershipEntry) => ownershipEntry.nodeId
            case None => -1
        }
    }

    def getPartitionsOwnedByNode(nodeId: Int): List[Int] = {
        //partitionToOwner.entries.filter(_._2.nodeId == nodeId).map(_._1).toList
        partitionToOwner.filter(_._2.nodeId == nodeId).keys.toList
    }

    /**
     * Merge the ownership map of the failed node with the current ownership map.
     * If the version of the ownership entry is higher than the current ownership entry, then update the ownership entry.
     * If the version is the same, then keep the ownership entry with the lower node id.
     * Otherwise, keep the current ownership entry.
     */
    def mergeOwnershipMap(other: scala.collection.mutable.Map[Int, OwnershipEntry]): Unit = {
        for ((partition, ownershipEntry) <- other) {
            partitionToOwner.get(partition) match {
                case Some(currentOwnershipEntry) =>
                    if (ownershipEntry.version > currentOwnershipEntry.version) {
                        partitionToOwner.put(partition, ownershipEntry)
                    } else if (ownershipEntry.version == currentOwnershipEntry.version && ownershipEntry.nodeId < currentOwnershipEntry.nodeId) {
                        partitionToOwner.put(partition, ownershipEntry)
                    }
                case None =>
                    partitionToOwner.put(partition, ownershipEntry)
            }
        }
        logger.debug(s"Merged ownership map: $partitionToOwner")
    }

    /**
     * Get list of partitions that current node should take ownership of.
     */
    def getNewOwnedPartitions(newOwnershipCrdt: scala.collection.mutable.Map[Int, OwnershipEntry]): List[Int] = {
        val newOwnedPartitions = scala.collection.mutable.ListBuffer[Int]()
        for ((partition, ownershipEntry) <- newOwnershipCrdt) {
            if (ownershipEntry.nodeId == nodeId) {
                partitionToOwner.get(partition) match {
                    case Some(currentOwnershipEntry) =>
                        // Partition is currently owned by another node
                        if (currentOwnershipEntry.nodeId != nodeId && currentOwnershipEntry.version < ownershipEntry.version) {
                            newOwnedPartitions += partition
                        }
                    case None =>
                        // Partition is not owned by any node yet
                        newOwnedPartitions += partition
                }
            }
        }
        newOwnedPartitions.toList
    }

    /**
     * Get list of partitions that current node should release ownership of.
     */
    def getPartitionsToRelease(newOwnershipCrdt: scala.collection.mutable.Map[Int, OwnershipEntry]): List[Int] = {
        val partitionsToRelease = scala.collection.mutable.ListBuffer[Int]()
        for ((partition, ownershipEntry) <- partitionToOwner) {
            if (ownershipEntry.nodeId == nodeId) {
                newOwnershipCrdt.get(partition) match {
                    case Some(newOwnershipEntry) =>
                        // Partition is currently owned by the current node
                        if (newOwnershipEntry.nodeId != nodeId && newOwnershipEntry.version > ownershipEntry.version) {
                            partitionsToRelease += partition
                        }
                    case None => {}
                }
            }
        }
        partitionsToRelease.toList
    }


    /**
     * Check if the current node is responsible for taking over the partitions of the failed node.
     */
    def checkOwnershipResponsibilityAfterFailure(failedNode: Int, failedNodes: List[Int]): Boolean = {
        var owner = failedNode
        while failedNodes.contains(owner) do
        // If the failed node is also the current node, then the current node is responsible for redistribution
            owner = (owner + 1) % N_NODES

        owner == nodeId
    }

    def getOwnershipMap: scala.collection.mutable.Map[Int, OwnershipEntry] = {
        partitionToOwner
    }
}
