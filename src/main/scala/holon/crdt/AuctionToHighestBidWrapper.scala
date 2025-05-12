package holon.crdt

import holon.example.{CRDT, Nexmark}
import org.apache.pekko.cluster.ddata.{LWWMap, SelfUniqueAddress}
import org.apache.pekko.cluster.ddata.LWWRegister.Clock

import java.util

/**
 * Keeps track of the highest bid per auction in a single LWWMap[auctionId -> price].  
 */
object AuctionToHighestBidWrapper extends CRDTWrapper[LWWMap[Long, Long], Map[Long, Long]] {

  type EventType = Nexmark.Events.Bid

  // Only accept Bid events
  override def checkType(ts: Nexmark.Events.TimeStampedEvent): Option[EventType] =
    ts.event match
      case b: EventType => Some(b)
      case _            => None

  // Use the event timestamp for the LWWRegister’s clock
  override def timeStamp(event: EventType): Long = event.dateTime

  // Start with an empty map
  override def empty(address: SelfUniqueAddress): LWWMap[Long, Long] = LWWMap.empty[Long, Long]

  /**
   * Update the map: for this auction, if this bid’s price is higher than the current value,
   * overwrite it; otherwise leave it unchanged.  
   */
  override def update(
                       crdt: LWWMap[Long, Long],
                       address: SelfUniqueAddress,
                       delta: EventType
                     ): LWWMap[Long, Long] = {
    
    implicit val priceClock: Clock[Long] = new Clock[Long] {
      override def apply(currentTimestamp: Long, value: Long): Long =
        value
    }
    
    val auctionId = delta.auction
    val newPrice  = delta.price

    // Fetch current highest (or 0 if none)
    val currentMax = crdt.get(auctionId).getOrElse(0L)

    if (newPrice > currentMax) {
      // puts a new register value with timestamp=timeStamp(delta)
      crdt.put(address, auctionId, newPrice)
    } else crdt
  }

  /**
   * Merge two LWWMaps by taking, per key, the entry with the later timestamp.  
   * This is just the built-in merge for ReplicatedData.  
   */
  override def merge(crdt1: LWWMap[Long, Long], crdt2: LWWMap[Long, Long]): LWWMap[Long, Long] =
    crdt1.merge(crdt2)

  /**
   * Expose the underlying map of auctionId -> highestPrice.  
   */
  override def value(crdt: LWWMap[Long, Long]): Map[Long, Long] =
    crdt.entries
}
