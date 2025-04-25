package holon.crdt

import holon.example.Nexmark
import holon.example.Nexmark.Events.TimeStampedEvent
import org.apache.pekko.cluster.ddata.{LWWRegister, SelfUniqueAddress}
import org.apache.pekko.cluster.ddata.LWWRegister.Clock
import upickle.legacy.{readBinary, writeBinary}

object HighestBidLWWRegisterWrapper extends CRDTWrapper[LWWRegister[Array[Byte]]] {

  // 1) Clock that extracts price from the serialized tuple
  val customPriceClock: Clock[Array[Byte]] = new Clock[Array[Byte]] {
    override def apply(currentTimestamp: Long, value: Array[Byte]): Long = {
      // decode to get (_, price)
      val (_, price) = readBinary[(Long, Long)](value)
      price
    }
  }

  // 2) Empty register holds serialized (0L, 0L)
  override def empty(address: SelfUniqueAddress): LWWRegister[Array[Byte]] =
    LWWRegister.create(address, writeBinary((0L, 0L)), customPriceClock)

  // 3) On each Bid, decode current price and only replace if higher
  override def increment(crdt: LWWRegister[Array[Byte]], address: SelfUniqueAddress, delta: TimeStampedEvent): LWWRegister[Array[Byte]] = 
    if (delta.event != Nexmark.Events.Bid) return crdt
    
    val delta_ = delta.asInstanceOf[Nexmark.Events.Bid]
    val (_, currentPrice) = readBinary[(Long, Long)](crdt.value)
    if (delta_.price > currentPrice) {
      val newBytes = writeBinary((delta_.bidder, delta_.price))
      crdt.withValue(address, newBytes, customPriceClock)
    } else crdt

  // 4) Standard CRDT merge
  override def merge(a: LWWRegister[Array[Byte]], b: LWWRegister[Array[Byte]]): LWWRegister[Array[Byte]] =
    a.merge(b)

  // 5) Pretty‐print by decoding the tuple
  override def value(crdt: LWWRegister[Array[Byte]]): String =
    val (bidder, price) = readBinary[(Long, Long)](crdt.value)
    s"Highest bid by bidder $bidder at price $price"
}