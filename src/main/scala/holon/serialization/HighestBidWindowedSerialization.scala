package holon.serialization

import holon.example.CRDT
import org.apache.pekko.cluster.ddata.{LWWMap, LWWRegister, ORSet}
import upickle.legacy.*
import upickle.legacy.readwriter

import scala.collection.mutable

object SerializationImplicits {

  // 1) LWWRegister[Array[Byte]] → Array[Byte]
  implicit val lwwRegisterBytesRW: ReadWriter[LWWRegister[Array[Byte]]] =
    readwriter[Array[Byte]].bimap[LWWRegister[Array[Byte]]](
      crdt  => CRDT.crdtToBinaryWithManifest("LWWRegister", crdt),
      bytes => CRDT.crdtFromBinaryWithManifest(bytes)._2
        .asInstanceOf[LWWRegister[Array[Byte]]]
    )

  implicit val orSetBytesRW: ReadWriter[ORSet[Array[Byte]]] =
    readwriter[Array[Byte]].bimap[ORSet[Array[Byte]]](
      // serializer: CRDT → bytes+manifest
      orset => CRDT.crdtToBinaryWithManifest("ORSet", orset),
      // deserializer: bytes+manifest → (type, CRDT), we cast back to ORSet[Array[Byte]]
      bytes => CRDT
        .crdtFromBinaryWithManifest(bytes)
        ._2
        .asInstanceOf[ORSet[Array[Byte]]]
    )

  // 2) window‐map: Map[windowTimestamp → (LWWRegister[Array[Byte]], closedFlag)]
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

  // 1) Serialize an LWWMap[String, Array[Byte]] via our CRDT manifest helpers
  implicit val lwwMapBytesRW: ReadWriter[LWWMap[String, Array[Byte]]] =
    readwriter[Array[Byte]].bimap[LWWMap[String, Array[Byte]]](
      crdt => CRDT.crdtToBinaryWithManifest("LWWMap", crdt),
      bytes => CRDT.crdtFromBinaryWithManifest(bytes)._2
        .asInstanceOf[LWWMap[String, Array[Byte]]]
    )

  // 2) window-map: Map[windowKey → (LWWMap[String,Array[Byte]], closedFlag)]
  implicit val windowMapLwwMapRW
  : ReadWriter[mutable.Map[Long, (LWWMap[String, Array[Byte]], Boolean)]] = {

    // intermediate immutable type
    type M = Map[Long, (LWWMap[String, Array[Byte]], Boolean)]

    readwriter[M].bimap[mutable.Map[Long, (LWWMap[String, Array[Byte]], Boolean)]](
      // to JSON: treat it as an immutable Map
      m => m.toMap,
      // from JSON: rebuild a mutable.Map
      m => mutable.Map.empty[Long, (LWWMap[String, Array[Byte]], Boolean)] ++ m
    )
  }
  
  

  // 3) (Optional) generic mutable‐map ReadWriter for other cases
//  implicit def mutableMapReadWriter[K: ReadWriter, V: ReadWriter]
//  : ReadWriter[mutable.Map[K, V]] =
//    readwriter[Map[K, V]].bimap[mutable.Map[K, V]](
//      _.toMap,
//      m => mutable.Map(m.toSeq: _*)
//    )
}
