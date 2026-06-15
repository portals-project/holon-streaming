package holon.streaming.recovery

import holon.utils.Logger
import holon.core.Config.*
import holon.streaming.messaging.messages.Heartbeat
import holon.core.Utils.RunThread
import holon.streaming.output.OutputCollectorImpl
import upickle.default.writeBinary

class FailureDetector(currentNodeId: Int, outputCollector: OutputCollectorImpl) {
    private val logger = Logger.apply("FailureDetector")
    Logger.setLevel("FailureDetector", "INFO")

    private val heartbeatMap = scala.collection.mutable.Map.empty[Int, Long]
    private var heartbeatCheckTime: Long = -1
    private var startCheckingForFailures = false
    private val heartbeatThread = RunThread(this.sendHeartbeats())


    private def sendHeartbeats(): Unit = {
        while(true) {
            try {
                logger.debug(s"Heartbeat: $currentNodeId")
                outputCollector.collect(CHN_CONTROL, List((writeBinary(0), writeBinary(Heartbeat(currentNodeId)))))
            } catch {
                case e: Exception =>
                    logger.debug(s"Failed to send heartbeat: ${e.getMessage}")
            }
            Thread.sleep(HEARTBEAT_INTERVAL)
        }
    }

    def stop(): Unit = {
        heartbeatThread.interrupt()
        heartbeatThread.join()
    }

    def setHeartbeat(nodeId: Int): Unit = {
        heartbeatMap.put(nodeId, System.currentTimeMillis())
    }

    def setHeartbeat(nodeId: Int, timestamp: Long): Unit = {
        heartbeatMap.put(nodeId, timestamp)
    }

    def setStartCheckingForFailures(): Unit = {
        if !startCheckingForFailures then
            logger.info("Starting to check for failed nodes")
            startCheckingForFailures = true

            // Initialize the heartbeat map
            for i <- 0 until N_NODES do
                if i != currentNodeId then
                    heartbeatMap.put(i, System.currentTimeMillis() + FAILURE_DETECTION_THRESHOLD)
    }

    /**
     * Check for failed nodes by comparing the current time with the last heartbeat.
     *
     * @return List of failed nodes. Empty list if no nodes have failed.
     */
    def checkNodeFailures(): Option[List[Int]] = {
        val t = System.currentTimeMillis()
        if startCheckingForFailures && heartbeatCheckTime > 0 && (t - heartbeatCheckTime) > FAILURE_DETECTION_THRESHOLD then {
            logger.debug(s"Checking for failed nodes $heartbeatMap")
            heartbeatCheckTime = System.currentTimeMillis()

            val failedNodes = heartbeatMap.filter { case (_, lastHeartbeat) =>
                (t - lastHeartbeat) > FAILURE_DETECTION_THRESHOLD
            }.keys

            return Some(failedNodes.toList)
        } else if heartbeatCheckTime < 0 then {
            heartbeatCheckTime = System.currentTimeMillis()
        }

        None
    }

}
