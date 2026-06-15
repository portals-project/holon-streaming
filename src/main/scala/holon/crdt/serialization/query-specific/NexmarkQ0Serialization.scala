package holon.crdt.serialization

import org.apache.pekko.cluster.ddata.GCounter
import upickle.legacy.{ReadWriter, readwriter}

import scala.collection.mutable

/**
 * Serialization for holon.examples.nexmark.data.Nexmark Query 0 (Q0) - Pass-through query.
 * Uses PassThroughLWWMapWrapper with GCounter serialization.
 */
object NexmarkQ0Serialization {

  // GCounter serialization
  implicit val gcounterRW: ReadWriter[GCounter] = readwriter[Array[Byte]].bimap[GCounter](
    gc => holon.crdt.CRDT.crdtToBinaryWithManifest("GCounter", gc),
    bytes => holon.crdt.CRDT.crdtFromBinaryWithManifest(bytes)._2.asInstanceOf[GCounter]
  )

  // Window map serialization for Q0
  implicit val windowMapRW: ReadWriter[mutable.Map[Long, (GCounter, Boolean)]] =
    readwriter[Map[Long, (GCounter, Boolean)]].bimap(
      (m: mutable.Map[Long, (GCounter, Boolean)]) => m.toMap,
      (m: Map[Long, (GCounter, Boolean)]) => mutable.Map(m.toSeq: _*)
    )
}
