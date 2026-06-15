package holon.crdt.serialization

import org.apache.pekko.cluster.ddata.{GSet, LWWMap, LWWRegister, ORMap}
import upickle.legacy.{ReadWriter, readwriter}

import scala.collection.mutable
import java.nio.ByteBuffer
import scala.jdk.CollectionConverters.*

/**
 * Serialization for holon.examples.nexmark.data.Nexmark Query 4 (Q4) - Auction to highest bid and category mapping.
 * Uses AuctionToHighestBidWrapper and AuctionToCategoryWrapper with GSet[(Long, Long)] serialization.
 */
object NexmarkQ4Serialization {

  trait ByteCodec[T] {
    def toBytes(t: T): Array[Byte]
    def fromBytes(bytes: Array[Byte]): T
  }

  object ByteCodec {
    def apply[T](implicit c: ByteCodec[T]): ByteCodec[T] = c

    implicit val arrayByteCodec: ByteCodec[Array[Byte]] = new ByteCodec[Array[Byte]] {
      def toBytes(a: Array[Byte]): Array[Byte] = a
      def fromBytes(bytes: Array[Byte]): Array[Byte] = bytes
    }

    implicit val longTupleCodec: ByteCodec[(Long, Long)] = new ByteCodec[(Long, Long)] {
      def toBytes(t: (Long, Long)): Array[Byte] = {
        val bb = ByteBuffer.allocate(java.lang.Long.BYTES * 2)
        bb.putLong(t._1).putLong(t._2)
        bb.array()
      }
      def fromBytes(bytes: Array[Byte]): (Long, Long) = {
        val bb = ByteBuffer.wrap(bytes)
        (bb.getLong, bb.getLong)
      }
    }
  }

  private def gsetFromSet[A](elems: Set[A]): GSet[A] =
    elems.foldLeft(GSet.empty[A])(_ + _)

  implicit def GSetByteCodedRW[T: ByteCodec]: ReadWriter[GSet[T]] =
    readwriter[Array[Byte]].bimap[GSet[T]](
      gsetT => {
        val rawBytesGSet: GSet[Array[Byte]] =
          gsetFromSet( gsetT.elements.map(ByteCodec[T].toBytes) )
        holon.crdt.CRDT.crdtToBinaryWithManifest("GSet", rawBytesGSet)
      },

      bytes => {
        val rawBytesGSet =
          holon.crdt.CRDT.crdtFromBinaryWithManifest(bytes)._2
            .asInstanceOf[GSet[Array[Byte]]]

        val tupleSet: Set[T] =
          rawBytesGSet.elements.map(ByteCodec[T].fromBytes)

        gsetFromSet(tupleSet)
      }
    )

  // Window-map RW for GSet[(Long, Long)]
  implicit def windowMapBytesRW
  : ReadWriter[mutable.Map[Long, (GSet[(Long, Long)], Boolean)]] = {

    type M = Map[Long, (GSet[(Long, Long)], Boolean)]

    readwriter[M].bimap[mutable.Map[Long, (GSet[(Long, Long)], Boolean)]](
      m => m.toMap,
      m => mutable.Map(m.toSeq: _*)
    )
  }

  implicit def byteArrayRW: ReadWriter[Array[Byte]] =
    readwriter[Seq[Byte]].bimap[Array[Byte]](
      arr => arr.toSeq, // write: Array[Byte] → Seq[Byte]
      seq => seq.toArray // read : Seq[Byte] → Array[Byte]
    )

  // summon the correct ReadWriter for GSet[(Long,Long)]
  implicit def rwTuple: ReadWriter[GSet[(Long, Long)]] =
    GSetByteCodedRW[(Long, Long)]

  // A RW for scala.collection.immutable.Set[T] (so upickle knows how to (de)serialize it):
  implicit def scalaSetReadWriter[T: ReadWriter]: ReadWriter[Set[T]] =
    readwriter[Seq[T]].bimap[Set[T]](
      _.toSeq, // serialize a Set[T] as a Seq[T]
      _.toSet // deserialize back into a Set[T]
    )

  // Now a RW for java.util.Set[T], mapping it ↔ Scala Set[T]:
  implicit def javaSetReadWriter[T: ReadWriter]: ReadWriter[java.util.Set[T]] =
    readwriter[Set[T]].bimap[java.util.Set[T]](
      jset => jset.asScala.toSet, // java.Set → Scala Set
      scalaSet => scalaSet.asJava // Scala Set → java.Set
    )

  // Window map for LWWMap[Long, Long]
  implicit val windowLWWMapRW: ReadWriter[mutable.Map[Long, (LWWMap[Long, Long], Boolean)]] =
    readwriter[Map[Long, (LWWMap[Long, Long], Boolean)]].bimap(
      (m: mutable.Map[Long, (LWWMap[Long, Long], Boolean)]) => m.toMap,
      (m: Map[Long, (LWWMap[Long, Long], Boolean)]) => mutable.Map(m.toSeq: _*)
    )

  implicit def rwLWWMap: ReadWriter[LWWMap[Long, Long]] = readwriter[Array[Byte]].bimap[LWWMap[Long, Long]](
    lwwmap => holon.crdt.CRDT.crdtToBinaryWithManifest("LWWMap", lwwmap),
    bytes => holon.crdt.CRDT.crdtFromBinaryWithManifest(bytes)._2.asInstanceOf[LWWMap[Long, Long]]
  )

  // Serialize/deserialize an ORMap[Long, LWWRegister[Long]] as a blob
  implicit def rwORMap: ReadWriter[ORMap[Long, LWWRegister[Long]]] =
    readwriter[Array[Byte]].bimap[ORMap[Long, LWWRegister[Long]]](
      ormap => holon.crdt.CRDT.crdtToBinaryWithManifest("ORMap", ormap),
      bytes  => holon.crdt.CRDT.crdtFromBinaryWithManifest(bytes)._2
        .asInstanceOf[ORMap[Long, LWWRegister[Long]]]
    )

  // Window map for ORMap[Long, LWWRegister[Long]]
  implicit val windowORMapRW
  : ReadWriter[mutable.Map[Long, (ORMap[Long, LWWRegister[Long]], Boolean)]] =
    readwriter[Map[Long, (ORMap[Long, LWWRegister[Long]], Boolean)]]
      .bimap[mutable.Map[Long, (ORMap[Long, LWWRegister[Long]], Boolean)]](
        m => m.toMap,
        m => mutable.Map(m.toSeq: _*)
      )
}
