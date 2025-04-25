package holon.serialization

import holon.example.CRDT
import org.apache.pekko.cluster.ddata.LWWRegister
import upickle.legacy._           // brings in ReadWriter for basic types, Maps, etc.
import upickle.legacy.readwriter // for our Array[Byte] bimap
import scala.collection.mutable

object SerializationImplicits {

  // 1) LWWRegister[Array[Byte]] → Array[Byte]
  implicit val lwwRegisterBytesRW: ReadWriter[LWWRegister[Array[Byte]]] =
    readwriter[Array[Byte]].bimap[LWWRegister[Array[Byte]]](
      crdt  => CRDT.crdtToBinaryWithManifest("LWWRegister", crdt),
      bytes => CRDT.crdtFromBinaryWithManifest(bytes)._2
        .asInstanceOf[LWWRegister[Array[Byte]]]
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

  // 3) (Optional) generic mutable‐map ReadWriter for other cases
//  implicit def mutableMapReadWriter[K: ReadWriter, V: ReadWriter]
//  : ReadWriter[mutable.Map[K, V]] =
//    readwriter[Map[K, V]].bimap[mutable.Map[K, V]](
//      _.toMap,
//      m => mutable.Map(m.toSeq: _*)
//    )
}
