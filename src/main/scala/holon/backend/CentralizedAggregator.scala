package holon.backend

import scala.collection.mutable

object CentralizedAggregator {
  // Assume a fixed number of partitions (for example, 4)
  val totalPartitions: Int = 4

  // Maintain state per window: mapping window id to partition counts.
  private val windowStates = mutable.Map.empty[Long, mutable.Map[Int, BigInt]]

  // Called by each partition when its local window aggregate is updated.
  def updateLocalAggregate(partition: Int, window: Long, localCount: BigInt): Unit = {
    val partitionMap = windowStates.getOrElseUpdate(window, mutable.Map.empty)
    partitionMap(partition) = localCount

    // When counts from all partitions have been received, compute and emit global aggregate.
    if (partitionMap.size == totalPartitions) {
      val globalCount = partitionMap.values.sum
      println(s"[Centralized] Global count for window $window: $globalCount")
      // Optionally, clear state for this window
      windowStates.remove(window)
    }
  }
}
