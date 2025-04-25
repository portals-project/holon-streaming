package holon.crdt

import holon.example.Nexmark
import org.apache.pekko.cluster.ddata.{GCounter, SelfUniqueAddress}

object BidCountGCounterWrapper extends CRDTWrapper[GCounter] {
  override def empty(address: SelfUniqueAddress): GCounter = GCounter.empty

  override def increment(crdt: GCounter, address: SelfUniqueAddress, delta: Nexmark.Events.TimeStampedEvent): GCounter = 
    if (delta.event == Nexmark.Events.Bid) {
      val delta_ = delta.asInstanceOf[Nexmark.Events.Bid]
      crdt.increment(address, 1L)
    } else crdt


  override def merge(a: GCounter, b: GCounter): GCounter =
    a.merge(b)

  override def value(crdt: GCounter): String =
    crdt.value.toString()
}

