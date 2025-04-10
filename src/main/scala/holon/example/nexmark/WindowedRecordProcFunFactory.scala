package holon.example.nexmark

import holon.ProcFun
import holon.ProcFunFactory
import holon.backend.AuctionWindowedRecordProcFun
import holon.crdt.GCounterWrapper
import holon.example.CRDT
import org.apache.pekko.cluster.ddata.GCounter
import upickle.default.*

// This factory creates AuctionWindowedRecordProcFun instances using AuctionGCounterWrapper
class WindowedRecordProcFunFactory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
    implicit val wrapper: GCounterWrapper.type = GCounterWrapper

    implicit val rw: ReadWriter[GCounter] = readwriter[Array[Byte]].bimap[GCounter](
      gc => CRDT.crdtToBinaryWithManifest("GCounter", gc),
      bytes => CRDT.crdtFromBinaryWithManifest(bytes)._2.asInstanceOf[GCounter]
    )

    // This ensures that a mutable Map[String, (GCounter, Boolean)] is encoded as a dictionary.
    implicit val windowMapRW: ReadWriter[scala.collection.mutable.Map[String, (GCounter, Boolean)]] =
      readwriter[Map[String, (GCounter, Boolean)]].bimap(
        (m: scala.collection.mutable.Map[String, (GCounter, Boolean)]) => m.toMap,
        (m: Map[String, (GCounter, Boolean)]) => scala.collection.mutable.Map(m.toSeq: _*)
      )

//    new WindowedRecordProcFun[GCounter](partition)
    // TODO: change this later
    new AuctionWindowedRecordProcFun[GCounter](partition)
  }
}
