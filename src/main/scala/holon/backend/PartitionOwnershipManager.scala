package holon.backend

import holon.*
import holon.example.CRDT.address
import org.apache.pekko.cluster.ddata.LWWRegister.Clock
import org.apache.pekko.cluster.ddata.{LWWMap, SelfUniqueAddress}

val ownershipClock: Clock[OwnershipEntry] = new Clock[OwnershipEntry] {
    override def apply(currentTimestamp: Long, value: OwnershipEntry): Long =
        value.version
}

class PartitionOwnershipManager(nodeId: Int) {

    val addr: SelfUniqueAddress = address(nodeId)
    var partitionToNodeId: LWWMap[Int, OwnershipEntry] = LWWMap.empty[Int, OwnershipEntry]
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
            partitionToNodeId = partitionToNodeId.put(addr, partition, ownershipEntry, ownershipClock)
        }
    }

    def setPartitionOwnership(partition: Int): Unit = {
        setPartitionOwnership(partition, nodeId)
    }

    def setPartitionOwnership(partition: Int, newOwnerId: Int): Unit = {
        val controlChannelOffset = controlChannelConsumer.get.offsets().head._2
        val ownershipEntry = OwnershipEntry(newOwnerId, controlChannelOffset)
        partitionToNodeId = partitionToNodeId.put(addr, partition, ownershipEntry, ownershipClock)
    }

    def getPartitionOwner(partition: Int): Int = {
        partitionToNodeId.get(partition) match {
            case Some(ownershipEntry) => ownershipEntry.nodeId
            case None => -1
        }
    }

    def getPartitionsOwnedByNode(nodeId: Int): List[Int] = {
        partitionToNodeId.entries.filter(_._2.nodeId == nodeId).map(_._1).toList
    }

    def mergeOwnershipMap(other: LWWMap[Int, OwnershipEntry]): Unit = {
        partitionToNodeId = partitionToNodeId.merge(other)
        logger.info(s"Merged ownership map: $partitionToNodeId")
    }

    def getOwnershipMap: LWWMap[Int, OwnershipEntry] = {
        partitionToNodeId
    }

}
