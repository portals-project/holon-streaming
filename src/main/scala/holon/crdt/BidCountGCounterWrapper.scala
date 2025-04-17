package holon.crdt

import holon.example.Nexmark
import org.apache.pekko.cluster.ddata.{GCounter, SelfUniqueAddress}

object BidCountGCounterWrapper extends CRDTWrapper[GCounter] {
  override def empty: GCounter = GCounter.empty

  override def increment(crdt: GCounter, address: SelfUniqueAddress, delta: Nexmark.Events.Bid): GCounter =
    crdt.increment(address, 1L)

  override def merge(a: GCounter, b: GCounter): GCounter =
    a.merge(b)

  // It probably makes more sense to return the value of the counter directly.
  // You could just use the return type `Any`, or make it a type parameter,
  // or return Array[Byte].
  override def value(crdt: GCounter): Any =
    crdt.value
}

