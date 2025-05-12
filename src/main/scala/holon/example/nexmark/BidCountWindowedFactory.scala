package holon.example.nexmark

import holon.ProcFun
import holon.ProcFunFactory
import holon.backend.WindowedRecordProcFun
import holon.crdt.BidCountGCounterWrapper
import org.apache.pekko.cluster.ddata.GCounter

// This factory creates AuctionWindowedRecordProcFun instances using AuctionGCounterWrapper
class BidCountWindowedFactory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
    implicit val wrapper: BidCountGCounterWrapper.type = BidCountGCounterWrapper
    
    new WindowedRecordProcFun[GCounter](partition)(
      crdt = wrapper,
      rw = holon.serialization.gcounterRW
    )
  }
}