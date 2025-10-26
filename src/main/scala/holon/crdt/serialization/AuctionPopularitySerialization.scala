package holon.serialization

import org.apache.pekko.cluster.ddata.GCounter
import upickle.legacy.{ReadWriter, readwriter}

import scala.collection.mutable

/**
 * General serialization for auction popularity tracking used across multiple queries.
 * This handles GCounter-based serialization for auction popularity metrics.
 */
object AuctionPopularitySerialization {

  implicit val mapGcounterRW: ReadWriter[Map[String, GCounter]] =
    readwriter[Map[String, Array[Byte]]].bimap[Map[String, GCounter]](
      // Convert a Map[String, GCounter] into a Map[String, Array[Byte]]
      m => m.map { case (k, gcounter) => k -> holon.crdt.CRDT.crdtToBinaryWithManifest("GCounter", gcounter) },
      // Convert back from Map[String, Array[Byte]] to Map[String, GCounter]
      bytesMap => bytesMap.map { case (k, bytes) =>
        k -> holon.crdt.CRDT.crdtFromBinaryWithManifest(bytes)._2.asInstanceOf[GCounter]
      }
    )

  // Serialization for the window state.
  // The window state is a mutable Map keyed by Long with values of type (Map[String, GCounter], Boolean)
  implicit val auctionPopularityWindowMapRW: ReadWriter[mutable.Map[Long, (Map[String, GCounter], Boolean)]] =
    readwriter[Map[Long, (Map[String, GCounter], Boolean)]].bimap(
      (m: mutable.Map[Long, (Map[String, GCounter], Boolean)]) => m.toMap,
      (m: Map[Long, (Map[String, GCounter], Boolean)]) => mutable.Map(m.toSeq: _*)
    )
}
