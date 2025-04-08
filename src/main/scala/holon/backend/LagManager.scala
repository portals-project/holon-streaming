package holon.backend

import holon.*
import holon.example.nexmark.Config.*

object LagManager {

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

    def getNodeWithMaxLag: Option[(Int, Long)] = {
        if (lagPerNode.isEmpty) {
            None
        } else {
            Some(lagPerNode.maxBy(_._2))
        }
    }

    /**
     * Returns partition with the largest lag and its lag.
     * Empty if node has no partition consumers.
     */
    def getPartitionWithMaxLag(consumerPerPartition: scala.collection.mutable.Map[Int, (Byte, LogConsumer)]): Option[(Int, Long)] = {
        val filteredConsumers = consumerPerPartition.view.filterKeys(partitionId => partitionId != BROADCAST_PARTITION_ID && partitionId != CONTROL_PARTITION_ID)
        if (filteredConsumers.isEmpty) {
            None
        } else {
            var maxPartitionId = -1
            var maxLag = -1L

            for ((partitionId, (_, consumer)) <- filteredConsumers) {
                val lag = consumer.lag()
                if (lag > maxLag) {
                    maxPartitionId = partitionId
                    maxLag = lag
                }
            }

            Some(maxPartitionId, maxLag)
        }
    }

    def calculateCurrentLagIfRequired(consumerPerPartition: scala.collection.mutable.Map[Int, (Byte, LogConsumer)]): Unit = {
        val t = System.currentTimeMillis()
        if (t - lagCalculationTime > LAG_CALCULATION_INTERVAL) {
            lagCalculationTime = t
            calculateCurrentLag(consumerPerPartition)
            logger.debug(s"Current lag: $currentLag and lag per node: $lagPerNode")
        }
    }

    private def calculateCurrentLag(consumerPerPartition: scala.collection.mutable.Map[Int, (Byte, LogConsumer)]): Unit = {
        currentLag = consumerPerPartition.map { case (partitionId, (_, consumer)) =>
            consumer.lag()
        }.sum
    }

}
