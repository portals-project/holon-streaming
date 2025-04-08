package holon.backend

import org.apache.pekko.cluster.ddata.{GCounter, SelfUniqueAddress}
import upickle.default.*

object DistributedCounter { // Our counter is now an Akka GCounter.
  type Counter = GCounter

  def emptyCounter: Counter = GCounter.empty

  // Increment the counter for the given node by the specified delta (default 1L)
  def updateCounter(counter: Counter, node: SelfUniqueAddress, delta: Long = 1L): Counter = counter.increment(node, delta)

  // Merge two GCounters using the built-in merge operation
  def mergeCounter(c1: Counter, c2: Counter): Counter = c1.merge(c2)

  // Return the current value of the counter as a BigInt
  def valueCounter(counter: Counter): BigInt = counter.value
}

case class RecordState(counter: DistributedCounter.Counter = DistributedCounter.emptyCounter)

object RecordState {
  implicit val rw: ReadWriter[RecordState] = macroRW
}