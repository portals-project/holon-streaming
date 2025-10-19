package holon.crdt

import holon.example.Nexmark
import org.apache.pekko.cluster.ddata.{GSet, ORSet, ReplicatedDelta, SelfUniqueAddress}
import upickle.legacy.{readBinary, writeBinary}
import scala.jdk.CollectionConverters._

object AuctionToCategoryWrapper extends CRDTWrapper[GSet[(Long, Long)], Set[(Long, Long)]] {

  type EventType = Nexmark.Events.Auction

  override def checkType(ts: Any): Option[EventType] =
    val timeStampedValue = ts.asInstanceOf[Nexmark.Events.TimeStampedEvent]
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
    // 1) apply
    val updated = update(crdt, address, event)

    // 2) grab its internal delta (if any)
    val maybeDelta: Option[ReplicatedDelta] = updated.delta

//    println(s"Delta for ${event.id} -> ${event.category}: $maybeDelta")

    // 3) reset so future .delta only sees new changes
    val cleared = updated.resetDelta

    // 4) return the cleared state + the real delta op
    (cleared, maybeDelta)
  }

  /**
   * Merge a received delta back into our local GSet.
   */
  override def mergeDelta(
                           crdt: GSet[(Long, Long)],
                           delta: ReplicatedDelta
                         ): GSet[(Long, Long)] = {
    // the GSet itself is its delta type: cast back to GSet[(Long,Long)]
    val op = delta.asInstanceOf[GSet[(Long, Long)]]
    // merge that delta‐op into your local state
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
