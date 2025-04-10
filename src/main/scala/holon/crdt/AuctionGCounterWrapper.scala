package holon.crdt

import org.apache.pekko.cluster.ddata.{GCounter, SelfUniqueAddress}

object AuctionGCounterWrapper extends CRDTWrapper[Map[String, GCounter]] {
  override def empty: Map[String, GCounter] = Map.empty

  // For backward compatibility, the non-keyed increment delegates to a default key (here "default").
  override def increment(crdt: Map[String, GCounter], address: SelfUniqueAddress, delta: Long): Map[String, GCounter] =
    increment(crdt, address, delta, "default")

  // Overloaded increment: increments the counter associated with the given auction (key).
  override def increment(crdt: Map[String, GCounter], address: SelfUniqueAddress, delta: Long, key: String): Map[String, GCounter] = {
    val counter = crdt.getOrElse(key, GCounter.empty)
    val updatedCounter = counter.increment(address, delta)
    crdt.updated(key, updatedCounter)
  }

  override def merge(a: Map[String, GCounter], b: Map[String, GCounter]): Map[String, GCounter] = {
    // Merge the two maps by taking the union of keys, merging counters for common keys.
    val allKeys = a.keySet ++ b.keySet
    allKeys.foldLeft(Map.empty[String, GCounter]) { (merged, key) =>
      val counterA = a.getOrElse(key, GCounter.empty)
      val counterB = b.getOrElse(key, GCounter.empty)
      merged.updated(key, counterA.merge(counterB))
    }
  }

  override def value(crdt: Map[String, GCounter]): BigInt = {
    // Sum the values of all counters in the map.
    value(crdt, "default")
    BigInt(0)
  }

  // The value method now returns a map from auction id to its corresponding bid count.
  override def value(crdt: Map[String, GCounter], string: String): Map[String, BigInt] = {
    crdt.map { case (auctionId, counter) => auctionId -> counter.value }
  }
}
