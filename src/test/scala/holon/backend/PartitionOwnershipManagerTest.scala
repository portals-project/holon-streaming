package holon.backend

import org.scalatest.funsuite.AnyFunSuite
import holon.example.nexmark.Config

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
}