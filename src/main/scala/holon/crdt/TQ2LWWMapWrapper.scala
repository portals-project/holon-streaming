package holon.crdt

import holon.example.taxi.TaxiProducer
import holon.example.taxi.TaxiProducer.Events.TimeStampedEvent
import org.apache.pekko.cluster.ddata.LWWRegister.Clock
import org.apache.pekko.cluster.ddata.{LWWMap, ORMap, ReplicatedDelta, SelfUniqueAddress}
import upickle.legacy.{readBinary, writeBinary}

object TQ2LWWMapWrapper extends CRDTWrapper[LWWMap[String, Array[Byte]], String] {
  type EventType = TaxiProducer.Events.TaxiTripEvent

  // our single map key
  private val Key = "value"

  // 1) A clock that uses the bid-price as the timestamp
  val customPriceClock: Clock[Array[Byte]] = new Clock[Array[Byte]] {
    override def apply(currentTimestamp: Long, value: Array[Byte]): Long = {
      val (_, price) = readBinary[(Long, Long)](value)
      price
    }
  }

  // todo: change any type to other generic
  override def checkType(ts: Any): Option[EventType] =
    val timeStampedValue = ts.asInstanceOf[TimeStampedEvent]
    timeStampedValue.event match {
      case b: EventType => Some(b)
      case _ => None
    }

  override def timeStamp(event: EventType): Long =
    event.dropoffTime

  // 3)
  override def empty(addr: SelfUniqueAddress): LWWMap[String, Array[Byte]] =
    // empty map has no entries
    LWWMap.empty[String, Array[Byte]]

  // 4) Plain-CRDT update: only overwrite if new price is higher
  override def update(
                       crdt: LWWMap[String, Array[Byte]],
                       address: SelfUniqueAddress,
                       delta: EventType
                     ): LWWMap[String, Array[Byte]] = {

    val currentBytes = crdt.get(Key).getOrElse(writeBinary((0L, 0L)))
    val (_, currentPrice) = readBinary[(Long, Long)](currentBytes)

    if (delta.totalAmount > currentPrice) {
      val newBytes = writeBinary(((delta.tripTimeSecs.toLong + delta.tripDistance.toLong), delta.totalAmount.toLong))
      // use LWWMap.put with our custom clock
      crdt.put(address, Key, newBytes, customPriceClock)
    } else crdt
  }

  // 5) Delta‐CRDT update: return the new map + optional delta
  override def updateWithDelta(
                                crdt: LWWMap[String, Array[Byte]],
                                address: SelfUniqueAddress,
                                event: EventType
                              ): (LWWMap[String, Array[Byte]], Option[ReplicatedDelta]) = {
    val updated = update(crdt, address, event)
    // peek at the delta
    val maybeDelta = updated.delta
    // reset so next time only new ops appear
    val cleared = updated.resetDelta
    (cleared, maybeDelta)
  }

  // 6) Full‐state merge (batch recovery)
  override def merge(
                      a: LWWMap[String, Array[Byte]],
                      b: LWWMap[String, Array[Byte]]
                    ): LWWMap[String, Array[Byte]] =
    a.merge(b)

  // 7) Single‐delta merge
  override def mergeDelta(
                           crdt: LWWMap[String, Array[Byte]],
                           delta: ReplicatedDelta
                         ): LWWMap[String, Array[Byte]] =
    crdt.mergeDelta(delta.asInstanceOf[ORMap.DeltaOp])

  // 8) Render for users
  override def value(crdt: LWWMap[String, Array[Byte]]): String =
    crdt.get(Key)
      .map(readBinary[(Long, Long)](_))
      .map { case (tripId, totalPrice) =>
        s"Trip with id: $tripId had total price: $totalPrice"
      }
      .getOrElse("no trips found")
}
