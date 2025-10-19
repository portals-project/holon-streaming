package holon.crdt

import holon.crdt.HighestBidLWWMapWrapper.{EventType, Key, customPriceClock, update}
import holon.example.Nexmark
import holon.example.Nexmark.Events
import holon.example.Nexmark.Events.TimeStampedEvent
import org.apache.pekko.cluster.ddata.{GCounter, LWWMap, ORMap, ReplicatedDelta, SelfUniqueAddress}
import org.apache.pekko.cluster.ddata.LWWRegister.Clock
import upickle.legacy.{readBinary, writeBinary}

import java.nio.ByteBuffer

object PassThroughLWWMapWrapper extends CRDTWrapper[GCounter, String] {
  type EventType = Nexmark.Events.Bid

  override def checkType(ts: Any): Option[EventType] =
    val timeStampedValue = ts.asInstanceOf[Nexmark.Events.TimeStampedEvent]
    timeStampedValue.event match {
      case a: EventType => Some(a)
      case _ => None
    }

  override def timeStamp(event: EventType): Long = event.dateTime

  override def empty(address: SelfUniqueAddress): GCounter = GCounter.empty

  override def update(crdt: GCounter, address: SelfUniqueAddress, delta: EventType): GCounter =
      crdt

  override def updateWithDelta(crdt: GCounter, address: SelfUniqueAddress, event: EventType): (GCounter, Option[ReplicatedDelta]) =
    GCounter.empty -> None

  override def merge(a: GCounter, b: GCounter): GCounter =
    a.merge(b)

  override def mergeDelta(crdt: GCounter, delta: ReplicatedDelta): GCounter =
    crdt.merge(delta.asInstanceOf[GCounter])

  override def value(crdt: GCounter): String =
    crdt.value.toString()
}