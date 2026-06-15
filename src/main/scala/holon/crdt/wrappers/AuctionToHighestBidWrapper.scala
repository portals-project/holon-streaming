package holon.crdt

import holon.examples.nexmark.data.Nexmark
import org.apache.pekko.cluster.ddata.{GSet, ReplicatedDelta, SelfUniqueAddress}
import scala.jdk.CollectionConverters._

object AuctionToHighestBidWrapper extends CRDTWrapper[GSet[(Long, Long)], Map[Long, Long]] {

  type EventType = holon.examples.nexmark.data.Nexmark.Events.Bid

  // only accept Bid events
  override def checkType(ts: Any): Option[EventType] =
    val timeStampedValue = ts.asInstanceOf[holon.examples.nexmark.data.Nexmark.Events.TimeStampedEvent]
    timeStampedValue.event match {
      case a: EventType => Some(a)
      case _ => None
    }

  // we still need a timestamp for watermarking, even though GSet ignores it
  override def timeStamp(event: EventType): Long = event.dateTime

  // start with an empty GSet
  override def empty(address: SelfUniqueAddress): GSet[(Long, Long)] =
    GSet.empty[(Long, Long)]

  /** Full‐state update: just add the new (auctionId→price) pair */
  override def update(
                       crdt: GSet[(Long, Long)],
                       address: SelfUniqueAddress,
                       event: EventType
                     ): GSet[(Long, Long)] =
    // check event.price against the current highest price for the auction
    if (crdt.getElements().asScala.exists { case (auction, price) => auction == event.auction && price >= event.price}) {
      // if the current price is higher or equal, do not update
      crdt
    } else
      // otherwise, add the new (auctionId, price) pair
      crdt.add((event.auction, event.price))

  /**
   * Delta‐aware update:
   * 1) add locally,
   * 2) grab its `.delta: Option[GSet[(Long,Long)]]`,
   * 3) clear it with `.resetDelta`,
   * 4) return (clearedState, deltaToBroadcast)
   */
  override def updateWithDelta(
                                crdt: GSet[(Long, Long)],
                                address: SelfUniqueAddress,
                                event: EventType
                              ): (GSet[(Long, Long)], Option[ReplicatedDelta]) = {
    val updated = update(crdt, address, event)
    val maybeDelta = updated.delta // get the delta of the update if present
    val cleared = updated.resetDelta // reset the delta to clear it
    (cleared, maybeDelta)
  }

  /** Merge a received GSet‐delta back into our local GSet */
  override def mergeDelta(
                           crdt: GSet[(Long, Long)],
                           delta: ReplicatedDelta
                         ): GSet[(Long, Long)] = {
    val op = delta.asInstanceOf[GSet[(Long, Long)]]
    crdt.mergeDelta(op)
  }

  /** Full‐state merge (unused in delta mode) */
  override def merge(
                      crdt1: GSet[(Long, Long)],
                      crdt2: GSet[(Long, Long)]
                    ): GSet[(Long, Long)] =
    crdt1.merge(crdt2)

  /**
   * At query time, collapse the set of (auction,price) pairs
   * into a Map[auction→highestPrice].
   */
  override def value(crdt: GSet[(Long, Long)]): Map[Long, Long] = {
    crdt.getElements().asScala
      .groupMap(_._1)(_._2) // Group the elements by auction and collect the prices
      .view
      .mapValues(_.max) // Pick the highest price for each auction
      .toMap
  }
}
