package holon.example.nexmark

import holon.ProcFun
import holon.ProcFunFactory
import holon.backend.WindowedRecordProcFun
import holon.crdt.GCounterWrapper
import org.apache.pekko.cluster.ddata.GCounter
import upickle.default.*

// This factory creates WindowedRecordProcFun instances using GCounter as the CRDT
class WindowedRecordProcFunFactory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
    implicit val wrapper: GCounterWrapper.type = GCounterWrapper
    implicit val rw: ReadWriter[GCounter] = readwriter[Array[Byte]].bimap[GCounter](
      gc => holon.example.CRDT.crdtToBinaryWithManifest("GCounter", gc),
      bytes => holon.example.CRDT.crdtFromBinaryWithManifest(bytes)._2.asInstanceOf[GCounter]
    )

    new WindowedRecordProcFun[GCounter](partition)
  }
}
