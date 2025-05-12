package holon.crdt

import holon.example.{CRDT, Nexmark}
import org.apache.pekko.cluster.ddata.LWWRegister.Clock
import org.apache.pekko.cluster.ddata.{GCounter, GSet, LWWRegister, SelfUniqueAddress}
import upickle.legacy.readBinary

object AuctionToCategoryWrapper extends CRDTWrapper[GSet[(Long, Long)], java.util.Set[(Long, Long)]] {
  type EventType = Nexmark.Events.Auction

  // Check if the event is a Bid.
  override def checkType(ts: Nexmark.Events.TimeStampedEvent): Option[EventType] =
    ts.event match
      case b: EventType => Some(b)
      case _            => None

  override def timeStamp(event: EventType): Long = event.dateTime

  override def empty(address: SelfUniqueAddress): GSet[(Long, Long)] = GSet.empty[(Long, Long)]

  // The increment method.
  override def update(
                       crdt: GSet[(Long, Long)],
                       address: SelfUniqueAddress,
                       delta: EventType): GSet[(Long, Long)] =
  
    val newAuction = (delta.id, delta.category)
    // Add the new auction to the existing GSet.
    crdt.add(newAuction)

  // Merging two maps of gsets.
  override def merge(
                      crdt1: GSet[(Long, Long)],
                      crdt2: GSet[(Long, Long)]): GSet[(Long, Long)] = {
    // Merge the two sets by taking the union of elements.
    val allElements = crdt1.elements ++ crdt2.elements
    allElements.foldLeft(GSet.empty[(Long, Long)]) { (merged, element) =>
      merged.add(element)
    }
  }

  // Return the CRDT value
  override def value(crdt: GSet[(Long, Long)]): java.util.Set[(Long, Long)] = {
    crdt.getElements()
  }
}