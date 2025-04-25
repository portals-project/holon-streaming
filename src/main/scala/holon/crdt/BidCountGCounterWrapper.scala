package holon.crdt

import holon.example.Nexmark
import holon.example.Nexmark.Events
import org.apache.pekko.cluster.ddata.{GCounter, SelfUniqueAddress}

object BidCountGCounterWrapper extends CRDTWrapper[GCounter] {
  type EventType = Nexmark.Events.Bid
  
  override def checkType(ts: Events.TimeStampedEvent): Option[EventType] =
    ts.event match
      case b: EventType => Some(b)
      case _      => None
      
  override def timeStamp(event: EventType): Long = event.dateTime
  
  override def empty(address: SelfUniqueAddress): GCounter = GCounter.empty

  override def increment(crdt: GCounter, address: SelfUniqueAddress, delta: EventType): GCounter =
      crdt.increment(address, 1L)


  override def merge(a: GCounter, b: GCounter): GCounter =
    a.merge(b)

  override def value(crdt: GCounter): String =
    crdt.value.toString()
}

