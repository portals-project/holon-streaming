package holon.backend

import holon.*
import holon.example.nexmark.Config.*

class FailureDetector(currentNodeId: Int) {

    private val HEARTBEAT_INTERVAL = 5_000L

    private val heartbeatMap = scala.collection.mutable.Map.empty[Int, Long]
    private var heartbeatCheckTime: Long = -1
    private val logger = Logger.apply("FailureDetector")
    private var startCheckingForFailures = false

    Logger.setLevel("FailureDetector", "INFO")

    // Initialize the heartbeat map
    for i <- 0 until N_NODES do
        if i != currentNodeId then
            heartbeatMap.put(i, System.currentTimeMillis())

    def setHeartbeat(nodeId: Int): Unit = {
        heartbeatMap.put(nodeId, System.currentTimeMillis())
    }

    def setHeartbeat(nodeId: Int, timestamp: Long): Unit = {
        heartbeatMap.put(nodeId, timestamp)
    }

    def setStartCheckingForFailures(): Unit = {
        startCheckingForFailures = true
    }

    /**
     * Check for failed nodes by comparing the current time with the last heartbeat.
     *
     * @return List of failed nodes. Empty list if no nodes have failed.
     */
    def checkNodeFailures(): List[Int] = {
        val t = System.currentTimeMillis()
        if startCheckingForFailures && heartbeatCheckTime > 0 && (t - heartbeatCheckTime) > HEARTBEAT_INTERVAL then {
            logger.debug(s"Checking for failed nodes $heartbeatMap")
            heartbeatCheckTime = System.currentTimeMillis()

            val failedNodes = heartbeatMap.filter { case (_, lastHeartbeat) =>
                (t - lastHeartbeat) > HEARTBEAT_INTERVAL
            }.keys

            return failedNodes.toList
        } else if heartbeatCheckTime < 0 then {
            heartbeatCheckTime = System.currentTimeMillis()
        }

        List.empty
    }

}
