package holon.backend

import org.scalatest.funsuite.AnyFunSuite

class FailureDetectorTest extends AnyFunSuite {

    test("setHeartbeat: should update heartbeat for a node") {
        val fd = new FailureDetector(0)
        fd.setStartCheckingForFailures()

        val nodeId = 1
        val timestamp = System.currentTimeMillis()
        fd.setHeartbeat(nodeId, timestamp)
        assert(fd.checkNodeFailures().isEmpty) // No failures expected
    }

    test("checkNodeFailures: should detect failed nodes") {
        val fd = new FailureDetector(0)
        fd.setStartCheckingForFailures()

        // Simulate a node with an outdated heartbeat
        val nodeId = 1
        fd.setHeartbeat(nodeId, System.currentTimeMillis() - 10_000)
        fd.checkNodeFailures()
        Thread.sleep(6_000) // Wait for the heartbeat interval to pass
        val failures = fd.checkNodeFailures()
        assert(failures.isDefined)
        assert(failures.get.contains(nodeId)) // Node 1 should be marked as failed
    }

    test("checkNodeFailures: should not check failures if not started") {
        val fd = new FailureDetector(0)

        // Simulate a node with an outdated heartbeat
        val nodeId = 1
        fd.setHeartbeat(nodeId, System.currentTimeMillis() - 10_000)

        val failures = fd.checkNodeFailures()
        assert(failures.isEmpty) // Failure detection not started, so no failures should be reported
    }

}
