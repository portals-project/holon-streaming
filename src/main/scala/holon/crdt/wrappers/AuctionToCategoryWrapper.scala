package holon.crdt

import holon.examples.nexmark.data.Nexmark
import org.apache.pekko.cluster.ddata.{GSet, ReplicatedDelta, SelfUniqueAddress}
import scala.jdk.CollectionConverters._

object AuctionToCategoryWrapper extends CRDTWrapper[GSet[(Long, Long)], Set[(Long, Long)]] {

  type EventType = holon.examples.nexmark.data.Nexmark.Events.Auction

  override def checkType(ts: Any): Option[EventType] =
    val timeStampedValue = ts.asInstanceOf[holon.examples.nexmark.data.Nexmark.Events.TimeStampedEvent]
    timeStampedValue.event match {
      case a: EventType => Some(a)
      case _ => None
    }

  override def timeStamp(event: EventType): Long = event.dateTime

  override def empty(address: SelfUniqueAddress): GSet[(Long, Long)] =
    GSet.empty[(Long, Long)]

  /** Full-state update: just add the new auction-category pair. */
  override def update(
                       crdt: GSet[(Long, Long)],
                       address: SelfUniqueAddress,
                       event: EventType
                     ): GSet[(Long, Long)] =
    crdt.add((event.id, event.category))

  /**
   * Delta-aware update:
   *
   *  1. apply the add locally,
   *  2. grab its delta (an ORSet.DeltaOp, which is a ReplicatedDelta),
   *  3. resetDelta so next .delta is fresh,
   *  4. return (clearedState, deltaToBroadcast).
   */
  override def updateWithDelta(
                                crdt: GSet[(Long, Long)],
                                address: SelfUniqueAddress,
                                event: EventType
                              ): (GSet[(Long, Long)], Option[ReplicatedDelta]) = {
    val updated = update(crdt, address, event)
    val maybeDelta: Option[ReplicatedDelta] = updated.delta
    val cleared = updated.resetDelta
    (cleared, maybeDelta)
  }

  /**
   * Merge a received delta back into our local GSet.
   */
  override def mergeDelta(
                           crdt: GSet[(Long, Long)],
                           delta: ReplicatedDelta
                         ): GSet[(Long, Long)] = {
    val op = delta.asInstanceOf[GSet[(Long, Long)]]
    crdt.mergeDelta(op)
  }

  /** Full-state merge (unchanged). */
  override def merge(
                      crdt1: GSet[(Long, Long)],
                      crdt2: GSet[(Long, Long)]
                    ): GSet[(Long, Long)] =
    crdt1.merge(crdt2)

  /** Expose as a normal Scala Set. */
  override def value(crdt: GSet[(Long, Long)]): Set[(Long, Long)] =
    crdt.getElements().asScala.toSet
}
