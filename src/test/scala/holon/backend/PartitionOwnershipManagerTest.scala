package holon.backend

import holon.Config
import holon.messages.OwnershipEntry
import org.scalatest.funsuite.AnyFunSuite

class PartitionOwnershipManagerTest extends AnyFunSuite {

    test("checkOwnershipResponsibilityAfterFailure: node is responsible for redistribution") {
        Config.N_NODES = 3
        val manager = new PartitionOwnershipManager(2)
        val failedNode = 0
        val failedNodes = List(0, 1)

        assert(manager.checkOwnershipResponsibilityAfterFailure(failedNode, failedNodes))
    }

    test("checkOwnershipResponsibilityAfterFailure: current node is not responsible for redistribution") {
        Config.N_NODES = 4
        val manager = new PartitionOwnershipManager(0)
        val failedNode = 1
        val failedNodes = List(1, 2)

        assert(!manager.checkOwnershipResponsibilityAfterFailure(failedNode, failedNodes))
    }

    test("checkOwnershipResponsibilityAfterFailure should handle circular node ids correctly") {
        Config.N_NODES = 3
        val manager = new PartitionOwnershipManager(0)
        val failedNode = 1
        val failedNodes = List(1,2)

        assert(manager.checkOwnershipResponsibilityAfterFailure(failedNode, failedNodes))
    }

    test("PartitionOwnershipManager: should work with multiple merge operations") {
        val ownershipManager = PartitionOwnershipManager(0)

        ownershipManager.initializePartitionOwnership(List(0, 1))

        val map2: scala.collection.mutable.Map[Int, OwnershipEntry] = scala.collection.mutable.Map()
        map2.put(2, OwnershipEntry(1, 0))
        map2.put(3, OwnershipEntry(1, 0))

        ownershipManager.mergeOwnershipMap(map2)
        println(ownershipManager.getOwnershipMap)


        val map3: scala.collection.mutable.Map[Int, OwnershipEntry] = scala.collection.mutable.Map()
        map3.put(4, OwnershipEntry(2, 0))
        map3.put(5, OwnershipEntry(2, 0))

        ownershipManager.mergeOwnershipMap(map3)
        assert(ownershipManager.getOwnershipMap.size == 6)
    }

    test("PartitionOwnershipManager: should pick the correct owner when version number is the same") {
        val ownershipManager = PartitionOwnershipManager(0)

        ownershipManager.initializePartitionOwnership(List(0, 1))

        val map2: scala.collection.mutable.Map[Int, OwnershipEntry] = scala.collection.mutable.Map()
        map2.put(0, OwnershipEntry(1, 0))
        map2.put(1, OwnershipEntry(1, 0))

        ownershipManager.mergeOwnershipMap(map2)
        assert(ownershipManager.getOwnershipMap.size == 2)
        assert(ownershipManager.getOwnershipMap(0).nodeId == 0)
        assert(ownershipManager.getOwnershipMap(1).nodeId == 0)
    }

    test("PartitionOwnershipManager: should get correct new owned partitions") {
        val ownershipManager = PartitionOwnershipManager(0)

        ownershipManager.initializePartitionOwnership(List(0, 1))

        val map2: scala.collection.mutable.Map[Int, OwnershipEntry] = scala.collection.mutable.Map()
        map2.put(2, OwnershipEntry(1, 0))
        map2.put(3, OwnershipEntry(1, 0))

        ownershipManager.mergeOwnershipMap(map2)

        val map3: scala.collection.mutable.Map[Int, OwnershipEntry] = scala.collection.mutable.Map()
        map3.put(2, OwnershipEntry(0, 4)) // New ownership entry for partition 2 with higher version
        map3.put(3, OwnershipEntry(1, 4))
        map3.put(4, OwnershipEntry(0, 4)) // Completely new partition 4

        val newOwnedPartitions = ownershipManager.getNewOwnedPartitions(map3)
        assert(newOwnedPartitions.size == 2)
        assert(newOwnedPartitions.contains(2))
        assert(newOwnedPartitions.contains(4))
        ownershipManager.mergeOwnershipMap(map3)

        val map4: scala.collection.mutable.Map[Int, OwnershipEntry] = scala.collection.mutable.Map()
        map4.put(3, OwnershipEntry(0, 4)) // New ownership entry for partition 3 with lower/equal verstion --> no update
        val newOwnedPartitions2 = ownershipManager.getNewOwnedPartitions(map4)
        assert(newOwnedPartitions2.size == 0)
    }

    test("PartitionOwnershipManager: should get correct partitions to release") {
        val ownershipManager = new PartitionOwnershipManager(0)

        // Initialize ownership for partitions 0, 1, and 2
        ownershipManager.initializePartitionOwnership(List(0, 1, 2))

        // Create a new ownership map where partition 1 and 2 are reassigned to another node
        val newOwnershipMap: scala.collection.mutable.Map[Int, OwnershipEntry] = scala.collection.mutable.Map()
        newOwnershipMap.put(1, OwnershipEntry(1, 1)) // New owner with higher version
        newOwnershipMap.put(2, OwnershipEntry(1, 2)) // New owner with higher version
        newOwnershipMap.put(0, OwnershipEntry(0, 0)) // Current node retains ownership of partition 0

        // Get partitions to release
        val partitionsToRelease = ownershipManager.getPartitionsToRelease(newOwnershipMap)

        // Assert that partitions 1 and 2 are identified for release
        assert(partitionsToRelease.size == 2)
        assert(partitionsToRelease.contains(1))
        assert(partitionsToRelease.contains(2))
    }
}