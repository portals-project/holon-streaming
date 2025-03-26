package holon.backend

import holon.*
import holon.example.CRDT.address
import org.apache.pekko.cluster.ddata.LWWRegister.{Clock, defaultClock}
import org.apache.pekko.cluster.ddata.{LWWMap, SelfUniqueAddress}

case class OwnershipEntry(nodeId: Int, version: Int)

val ownershipClock: Clock[OwnershipEntry] = new LWWRegister.Clock[OwnershipEntry] {
    override def apply(currentTimestamp: Long, value: OwnershipEntry): Long =
        value.version
}

class PartitionOwnershipManager(nodeId: Int) {

    val addr: SelfUniqueAddress = address(nodeId)
    var partitionToNodeId: LWWMap[Int, OwnershipEntry] = LWWMap.empty[Int, OwnershipEntry]
    var controlChannelConsumer: Option[LogConsumer] = None

    def setControlChannelConsumer(controlChannelConsumer: LogConsumer): Unit = {
        this.controlChannelConsumer = Some(controlChannelConsumer)
    }

    def initializePartitionOwnership(partitions: List[Int]): Unit = {
        for (partition <- partitions) {
            // Initialize with -1 so it does not overwrite existing ownership (in the case that node failed an rejoined)
            val ownershipEntry = OwnershipEntry(nodeId, -1)
            partitionToNodeId = partitionToNodeId.put(addr, partition, ownershipEntry, ownershipClock)
        }
    }

    def setPartitionOwnership(partition: Int): Unit = {
        setPartitionOwnership(partition, nodeId)
    }

    def setPartitionOwnership(partition: Int, newOwnerId: Int): Unit = {
        // TODO Update
        val ownershipEntry = OwnershipEntry(newOwnerId, partitionToNodeId.get(partition).map(_.version).getOrElse(0) + 1)
        partitionToNodeId = partitionToNodeId.put(addr, partition, ownershipEntry, ownershipClock)
    }

    def getOwnerOfPartition(partition: Int): Int = {
        partitionToNodeId.get(partition).getOrElse(-1)
    }

    def getPartitionsOwnedByNode(nodeId: Int): List[Int] = {
        partitionToNodeId.entries.filter(_._2 == nodeId).map(_._1).toList
    }

    def mergeOwnershipMap(other: LWWMap[Int, OwnershipEntry]): Unit = {
        partitionToNodeId = partitionToNodeId.merge(other)
    }

    def getOwnershipMap: LWWMap[Int, OwnershipEntry] = {
        partitionToNodeId
    }

}
