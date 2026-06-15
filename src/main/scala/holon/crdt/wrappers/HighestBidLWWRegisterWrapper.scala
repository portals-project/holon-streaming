package holon.crdt

import holon.examples.nexmark.data.Nexmark
import org.apache.pekko.cluster.ddata.{LWWMap, ORMap, ReplicatedDelta, SelfUniqueAddress}
import org.apache.pekko.cluster.ddata.LWWRegister.Clock
import upickle.legacy.{readBinary, writeBinary}

object HighestBidLWWRegisterWrapper extends CRDTWrapper[LWWMap[String, Array[Byte]], String] {
  type EventType = holon.examples.nexmark.data.Nexmark.Events.Bid

  // our single map key
  private val Key = "value"

  // 1) A clock that uses the bid-price as the timestamp
  val customPriceClock: Clock[Array[Byte]] = new Clock[Array[Byte]] {
    override def apply(currentTimestamp: Long, value: Array[Byte]): Long = {
      val (_, price) = readBinary[(Long, Long)](value)
      price
    }
  }

  // 2) Only accept Bid events
  override def checkType(ts: Any): Option[EventType] =
    val timeStampedValue = ts.asInstanceOf[holon.examples.nexmark.data.Nexmark.Events.TimeStampedEvent]
    timeStampedValue.event match {
      case a: EventType => Some(a)
      case _ => None
    }

  override def timeStamp(event: EventType): Long =
    event.dateTime

  // 3) Start from bidder=0, price=0
  override def empty(addr: SelfUniqueAddress): LWWMap[String, Array[Byte]] =
    // empty map has no entries
    LWWMap.empty[String, Array[Byte]]

  // 4) Plain-holon.crdt.CRDT update: only overwrite if new price is higher
  override def update(
                       crdt: LWWMap[String, Array[Byte]],
                       address: SelfUniqueAddress,
                       delta: EventType
                     ): LWWMap[String, Array[Byte]] = {
    val currentBytes = crdt.get(Key).getOrElse(writeBinary((0L, 0L)))
    val (_, currentPrice) = readBinary[(Long, Long)](currentBytes)

    if (delta.price > currentPrice) {
      val newBytes = writeBinary((delta.bidder, delta.price))
      // use LWWMap.put with our custom clock
      crdt.put(address, Key, newBytes, customPriceClock)
    } else crdt
  }

  // 5) Delta‐holon.crdt.CRDT update: return the new map + optional delta
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
      .map { case (bidder, price) =>
        s"Highest bid by bidder $bidder at price $price"
      }
      .getOrElse("no bids yet")
}
