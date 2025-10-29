package holon.crdt

import holon.examples.nexmark.data.Nexmark
import holon.examples.nexmark.data.Nexmark.Events
import org.apache.pekko.cluster.ddata.{GCounter, SelfUniqueAddress, ReplicatedDelta}

object BidCountGCounterWrapper extends CRDTWrapper[GCounter, String] {
  type EventType = holon.examples.nexmark.data.Nexmark.Events.Bid

  override def checkType(tsEvent: Any): Option[EventType] =
    tsEvent match
      case ts: Events.TimeStampedEvent =>
        ts.event match
          case b: EventType => Some(b)
          case _      => None
      case _ => None

  override def timeStamp(event: EventType): Long = event.dateTime

  override def empty(address: SelfUniqueAddress): GCounter = GCounter.empty

  override def update(crdt: GCounter, address: SelfUniqueAddress, delta: EventType): GCounter =
      crdt.increment(address, 1L)

  override def merge(a: GCounter, b: GCounter): GCounter =
    a.merge(b)

  override def value(crdt: GCounter): String =
    crdt.value.toString()

  override def updateWithDelta(crdt: GCounter, addr: SelfUniqueAddress, e: EventType): (GCounter, Option[ReplicatedDelta]) = {
    val updated = update(crdt, addr, e)
    (updated, None)
  }

  override def mergeDelta(crdt: GCounter, delta: ReplicatedDelta): GCounter = {
    crdt // No delta support for this CRDT
  }
}
