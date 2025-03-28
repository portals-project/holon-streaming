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
}