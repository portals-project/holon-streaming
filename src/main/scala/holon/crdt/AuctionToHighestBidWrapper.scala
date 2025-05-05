package holon.crdt

import holon.example.{CRDT, Nexmark}
import org.apache.pekko.cluster.ddata.LWWRegister.Clock
import org.apache.pekko.cluster.ddata.{GCounter, GSet, LWWRegister, SelfUniqueAddress}

object AuctionToHighestBidWrapper extends CRDTWrapper[GSet[(Long, Long)], java.util.Set[(Long, Long)]] {
  type EventType = Nexmark.Events.Bid

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
    // Create a new GSet with the auction ID and price.
    val newBid = (delta.auction, delta.price)
    // Only add the new bid if it is higher than the current max in the set.
    val currentMaxBid = crdt.elements.find(_._1 == delta.auction).map(_._2).getOrElse(0L)

    if (newBid._2 > currentMaxBid)
//      print(s" --- Address: $address Adding new bid: $newBid because it's higher than current max bid: $currentMaxBid --- \n")
      // Add the new bid to the existing GSet.
      crdt.add(newBid)
    else
      // If the new bid is not higher, return the existing GSet.
      crdt

  // Merging two maps of GCounters.
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