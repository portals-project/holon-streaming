package holon.serialization

import holon.example.CRDT
import org.apache.pekko.cluster.ddata.GCounter
import upickle.legacy.{ReadWriter, readwriter}

// This ensures that a mutable Map[String, (GCounter, Boolean)] is encoded as a dictionary.
implicit val windowMapRW: ReadWriter[scala.collection.mutable.Map[String, (GCounter, Boolean)]] =
  readwriter[Map[String, (GCounter, Boolean)]].bimap(
    (m: scala.collection.mutable.Map[String, (GCounter, Boolean)]) => m.toMap,
    (m: Map[String, (GCounter, Boolean)]) => scala.collection.mutable.Map(m.toSeq: _*)
  )

implicit val gcounterRW: ReadWriter[GCounter] = readwriter[Array[Byte]].bimap[GCounter](
  gc => CRDT.crdtToBinaryWithManifest("GCounter", gc),
  bytes => CRDT.crdtFromBinaryWithManifest(bytes)._2.asInstanceOf[GCounter]
)

// Generic implicit for mutable maps
implicit def mutableMapReadWriter[K: ReadWriter, V: ReadWriter]: ReadWriter[scala.collection.mutable.Map[K, V]] =
  readwriter[Map[K, V]].bimap[scala.collection.mutable.Map[K, V]](
    _.toMap,
    m => scala.collection.mutable.Map.empty[K, V] ++ m
  )