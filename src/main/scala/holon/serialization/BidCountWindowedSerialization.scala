package holon.serialization

import holon.example.CRDT
import org.apache.pekko.cluster.ddata.GCounter
import upickle.default.*

// This ensures that a mutable Map[String, (GCounter, Boolean)] is encoded as a dictionary.
// implicit val windowMapRW: ReadWriter[scala.collection.mutable.Map[String, (GCounter, Boolean)]] =
//   readwriter[Map[String, (GCounter, Boolean)]].bimap(
//     (m: scala.collection.mutable.Map[String, (GCounter, Boolean)]) => m.toMap,
//     (m: Map[String, (GCounter, Boolean)]) => scala.collection.mutable.Map(m.toSeq: _*)
//   )


// Serialization may be simpler, something like this should work, although it doesn't compile due to the rest of the project
// not compiling now after some changes...
given ReadWriter[scala.collection.mutable.Map[String, Tuple2[GCounter, Boolean]]] = macroRW
given [K, V](using ReadWriter[K], ReadWriter[V]): ReadWriter[scala.collection.mutable.Map[K, V]] = readwriter[String].bimap(
  (m: scala.collection.mutable.Map[K, V]) => write(m.toMap),
  (m: String) => scala.collection.mutable.Map.from(upickle.default.read[Map[K, V]](m))
)
given a: ReadWriter[Tuple2[String, Tuple2[GCounter, Boolean]]] = macroRW
given b: ReadWriter[Tuple2[GCounter, Boolean]] = macroRW
given c: ReadWriter[GCounter] = readwriter[Array[Byte]].bimap[GCounter](
  gc => CRDT.crdtToBinaryWithManifest("GCounter", gc),
  bytes => CRDT.crdtFromBinaryWithManifest(bytes)._2.asInstanceOf[GCounter]
)

// implicit val gcounterRW: ReadWriter[GCounter] = readwriter[Array[Byte]].bimap[GCounter](
//   gc => CRDT.crdtToBinaryWithManifest("GCounter", gc),
//   bytes => CRDT.crdtFromBinaryWithManifest(bytes)._2.asInstanceOf[GCounter]
// )

// // Generic implicit for mutable maps
// implicit def mutableMapReadWriter[K: ReadWriter, V: ReadWriter]: ReadWriter[scala.collection.mutable.Map[K, V]] =
//   readwriter[Map[K, V]].bimap[scala.collection.mutable.Map[K, V]](
//     _.toMap,
//     m => scala.collection.mutable.Map.empty[K, V] ++ m
//   )