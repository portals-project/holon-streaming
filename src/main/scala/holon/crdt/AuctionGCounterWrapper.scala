package holon.crdt

import holon.example.Nexmark
import org.apache.pekko.cluster.ddata.{GCounter, SelfUniqueAddress}

object AuctionGCounterWrapper extends CRDTWrapper[Map[String, GCounter]] {
  override def empty: Map[String, GCounter] = Map.empty

  // For backward compatibility, the non-keyed increment delegates to a default key (here "default").
  override def increment(crdt: Map[String, GCounter], address: SelfUniqueAddress, delta: Nexmark.Events.Bid): Map[String, GCounter] =
    val auction: Long = delta.auction
    val counter = crdt.getOrElse(auction.toString, GCounter.empty)
    val updatedCounter = counter.increment(address, 1L)
    crdt.updated(auction.toString, updatedCounter)

  override def merge(a: Map[String, GCounter], b: Map[String, GCounter]): Map[String, GCounter] = {
    // Merge the two maps by taking the union of keys, merging counters for common keys.
    val allKeys = a.keySet ++ b.keySet
    allKeys.foldLeft(Map.empty[String, GCounter]) { (merged, key) =>
      val counterA = a.getOrElse(key, GCounter.empty)
      val counterB = b.getOrElse(key, GCounter.empty)
      merged.updated(key, counterA.merge(counterB))
    }
  }

  // The value method now returns a map from auction id to its corresponding bid count.
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
