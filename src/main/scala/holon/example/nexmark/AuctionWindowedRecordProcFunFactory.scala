package holon.example.nexmark

import holon.ProcFun
import holon.ProcFunFactory
import holon.backend.AuctionWindowedRecordProcFun
import holon.crdt.AuctionGCounterWrapper
import holon.example.CRDT
import org.apache.pekko.cluster.ddata.GCounter
import upickle.default._

// This factory creates AuctionWindowedRecordProcFun instances using AuctionGCounterWrapper
class AuctionWindowedRecordProcFunFactory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
    // Use the new AuctionGCounterWrapper for mapping auctions to their bid counters.
    implicit val wrapper: AuctionGCounterWrapper.type = AuctionGCounterWrapper

    implicit val rw: ReadWriter[Map[String, GCounter]] =
      readwriter[Map[String, Array[Byte]]].bimap[Map[String, GCounter]](
        // Convert a Map[String, GCounter] into a Map[String, Array[Byte]]
        m => m.map { case (k, gcounter) => k -> CRDT.crdtToBinaryWithManifest("GCounter", gcounter) },
        // Convert back from Map[String, Array[Byte]] to Map[String, GCounter]
        bytesMap => bytesMap.map { case (k, bytes) =>
          k -> CRDT.crdtFromBinaryWithManifest(bytes)._2.asInstanceOf[GCounter]
        }
      )


    // Serialization for the window state.
    // The window state is a mutable Map keyed by Long with values of type (Map[String, GCounter], Boolean)
    implicit val windowMapRW: ReadWriter[scala.collection.mutable.Map[Long, (Map[String, GCounter], Boolean)]] =
      readwriter[Map[Long, (Map[String, GCounter], Boolean)]].bimap(
        (m: scala.collection.mutable.Map[Long, (Map[String, GCounter], Boolean)]) => m.toMap,
        (m: Map[Long, (Map[String, GCounter], Boolean)]) => scala.collection.mutable.Map(m.toSeq: _*)
      )

    // Instantiate the AuctionWindowedRecordProcFun with T = Map[String, GCounter]
    new AuctionWindowedRecordProcFun[Map[String, GCounter]](partition)
  }
}
