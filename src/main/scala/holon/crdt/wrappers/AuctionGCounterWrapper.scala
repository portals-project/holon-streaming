package holon.crdt

import holon.examples.nexmark.data.Nexmark
import org.apache.pekko.cluster.ddata.{GCounter, SelfUniqueAddress, ReplicatedDelta}

object AuctionGCounterWrapper extends CRDTWrapper[Map[String, GCounter], String] {
  type EventType = Nexmark.Events.Auction

  // Check if the event is an Auction.
  override def checkType(tsEvent: Any): Option[EventType] =
    tsEvent match
      case ts: Nexmark.Events.TimeStampedEvent =>
        ts.event match
          case a: EventType => Some(a)
          case _            => None
      case _ => None

  override def timeStamp(event: EventType): Long = event.dateTime

  override def empty(address: SelfUniqueAddress): Map[String, GCounter] = Map.empty

  // The increment method.
  override def update(crdt: Map[String, GCounter], address: SelfUniqueAddress, delta: EventType): Map[String, GCounter] =
    val category: Long = delta.category
    val counter = crdt.getOrElse(category.toString, GCounter.empty)
    val updatedCounter = counter.increment(address, 1L)
    crdt.updated(category.toString, updatedCounter)

  // Merging two maps of GCounters.
  override def merge(a: Map[String, GCounter], b: Map[String, GCounter]): Map[String, GCounter] = {
    // Merge the two maps by taking the union of keys, merging counters for common keys.
    val allKeys = a.keySet ++ b.keySet
    allKeys.foldLeft(Map.empty[String, GCounter]) { (merged, key) =>
      val counterA = a.getOrElse(key, GCounter.empty)
      val counterB = b.getOrElse(key, GCounter.empty)
      merged.updated(key, counterA.merge(counterB))
    }
  }

  // The value method now returns a string containing.
  override def value(crdt: Map[String, GCounter]): String = {
    // Find the category with the maximum auction count.
    val maxCategory = crdt.maxByOption { case (_, counter) => counter.value }
    maxCategory match {
      case Some((categoryId, counter)) =>
        val categoryCount = counter.value
        s"Category ID: $categoryId, Auction Count: $categoryCount"
      case None =>
        "No categories found."
    }
  }

  override def updateWithDelta(crdt: Map[String, GCounter], addr: SelfUniqueAddress, e: EventType): (Map[String, GCounter], Option[ReplicatedDelta]) = {
    val updated = update(crdt, addr, e)
    (updated, None)
  }

  override def mergeDelta(crdt: Map[String, GCounter], delta: ReplicatedDelta): Map[String, GCounter] = {
    crdt // No delta support for this CRDT
  }
}
