package holon.example.nexmark

import holon.ProcFun
import holon.ProcFunFactory
import holon.backend.WindowedRecordProcFun
import holon.crdt.GCounterWrapper
import holon.example.CRDT
import org.apache.pekko.cluster.ddata.GCounter
import upickle.default.*

// This factory creates AuctionWindowedRecordProcFun instances using AuctionGCounterWrapper
class BidCountWindowedFactory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
    implicit val wrapper: GCounterWrapper.type = GCounterWrapper
    
    new WindowedRecordProcFun[GCounter](partition)(
      crdt = wrapper,
      rw = holon.serialization.gcounterRW
    )
  }
}
