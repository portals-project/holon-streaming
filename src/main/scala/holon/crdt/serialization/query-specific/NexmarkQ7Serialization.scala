package holon.crdt.serialization

import org.apache.pekko.cluster.ddata.{LWWMap, LWWRegister, ORSet}
import upickle.legacy.*
import upickle.legacy.readwriter

import scala.collection.mutable

/**
 * Serialization for holon.examples.nexmark.data.Nexmark Query 7 (Q7) - Highest bid tracking.
 * Uses HighestBidLWWMapWrapper with LWWMap[String, Array[Byte]] serialization.
 */
object NexmarkQ7Serialization {

  // LWWRegister[Array[Byte]] → Array[Byte]
  implicit val lwwRegisterBytesRW: ReadWriter[LWWRegister[Array[Byte]]] =
    readwriter[Array[Byte]].bimap[LWWRegister[Array[Byte]]](
      crdt  => holon.crdt.CRDT.crdtToBinaryWithManifest("LWWRegister", crdt),
      bytes => holon.crdt.CRDT.crdtFromBinaryWithManifest(bytes)._2
        .asInstanceOf[LWWRegister[Array[Byte]]]
    )

  implicit val orSetBytesRW: ReadWriter[ORSet[Array[Byte]]] =
    readwriter[Array[Byte]].bimap[ORSet[Array[Byte]]](
      // serializer: holon.crdt.CRDT → bytes+manifest
      orset => holon.crdt.CRDT.crdtToBinaryWithManifest("ORSet", orset),
      // deserializer: bytes+manifest → (type, holon.crdt.CRDT), we cast back to ORSet[Array[Byte]]
      bytes => holon.crdt.CRDT
        .crdtFromBinaryWithManifest(bytes)
        ._2
        .asInstanceOf[ORSet[Array[Byte]]]
    )

  // Window‐map: Map[windowTimestamp → (LWWRegister[Array[Byte]], closedFlag)]
  implicit val windowMapBytesRW
  : ReadWriter[mutable.Map[Long, (LWWRegister[Array[Byte]], Boolean)]] = {

    type M = Map[Long, (LWWRegister[Array[Byte]], Boolean)]

    readwriter[M].bimap[mutable.Map[Long, (LWWRegister[Array[Byte]], Boolean)]](
      // to JSON: treat it as an immutable Map
      m => m.toMap,
      // from JSON: rebuild a mutable.Map
      m => mutable.Map(m.toSeq: _*)
    )
  }

  // Serialize an LWWMap[String, Array[Byte]] via our holon.crdt.CRDT manifest helpers
  implicit val lwwMapBytesRW: ReadWriter[LWWMap[String, Array[Byte]]] =
    readwriter[Array[Byte]].bimap[LWWMap[String, Array[Byte]]](
      crdt => holon.crdt.CRDT.crdtToBinaryWithManifest("LWWMap", crdt),
      bytes => holon.crdt.CRDT.crdtFromBinaryWithManifest(bytes)._2
        .asInstanceOf[LWWMap[String, Array[Byte]]]
    )

  // Window-map: Map[windowKey → (LWWMap[String,Array[Byte]], closedFlag)]
  implicit val windowLWWMapBytesRW: ReadWriter[mutable.Map[Long, (LWWMap[String, Array[Byte]], Boolean)]] = {
    type M = Map[Long, (LWWMap[String, Array[Byte]], Boolean)]

    readwriter[M].bimap[mutable.Map[Long, (LWWMap[String, Array[Byte]], Boolean)]](
      m => m.toMap,
      m => mutable.Map(m.toSeq: _*)
    )
  }
}
