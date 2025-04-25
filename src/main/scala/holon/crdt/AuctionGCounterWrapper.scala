package holon.crdt

import holon.example.Nexmark
import org.apache.pekko.cluster.ddata.{GCounter, SelfUniqueAddress}

object AuctionGCounterWrapper extends CRDTWrapper[Map[String, GCounter]] {
  type EventType = Nexmark.Events.Bid
  
  // Check if the event is a Bid.
  override def checkType(ts: Nexmark.Events.TimeStampedEvent): Option[EventType] =
    ts.event match
      case b: EventType => Some(b)
      case _            => None
  
  override def timeStamp(event: EventType): Long = event.dateTime
  
  override def empty(address: SelfUniqueAddress): Map[String, GCounter] = Map.empty
  
  // The increment method.
  override def increment(crdt: Map[String, GCounter], address: SelfUniqueAddress, delta: EventType): Map[String, GCounter] =
    val auction: Long = delta.auction
    val counter = crdt.getOrElse(auction.toString, GCounter.empty)
    val updatedCounter = counter.increment(address, 1L)
    crdt.updated(auction.toString, updatedCounter)

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
    // Find the auction with the maximum bid count.
    val maxAuction = crdt.maxByOption { case (_, counter) => counter.value }
    maxAuction match {
      case Some((auctionId, counter)) =>
        val auctionCount = counter.value
        s"Auction ID: $auctionId, Bid Count: $auctionCount"
      case None =>
        "No auctions found."
    }
  }
}