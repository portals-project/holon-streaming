package holon.backend

import holon.*
import holon.Logger

class LagManager {

    // Keeps track of the current lag for this node across all partitions
    private val LAG_CALCULATION_INTERVAL = 1_000
    private var lagCalculationTime = System.currentTimeMillis()
    private val lagPerNode = scala.collection.mutable.Map.empty[Int, Long]
    private var currentLag = 0L
    private val logger = Logger.apply("LagManager")

    Logger.setLevel("LagManager", "INFO")

    def getCurrentLag: Long = {
        currentLag
    }

    def updateLag(nodeId: Int, lag: Long): Unit = {
        lagPerNode(nodeId) = lag
    }

    def getNodeWithMaxLag: Int = {
        lagPerNode.maxBy(_._2)._1
    }

    def calculateCurrentLagIfRequired(consumerPerPartition: scala.collection.mutable.Map[Int, (Byte, LogConsumer)]) : Unit = {
        val t = System.currentTimeMillis()
        if (t - lagCalculationTime > LAG_CALCULATION_INTERVAL) {
            lagCalculationTime = t
            calculateCurrentLag(consumerPerPartition)
            logger.info(s"Current lag: $currentLag and lag per node: $lagPerNode")
        }
    }

    private def calculateCurrentLag(consumerPerPartition: scala.collection.mutable.Map[Int, (Byte, LogConsumer)]) : Unit = {
        currentLag = consumerPerPartition.map { case (partitionId, (_, consumer)) =>
            consumer.lag()
        }.sum
    }

}
