package holon.example.nexmark

import holon.ProcFun
import holon.ProcFunFactory
import holon.backend.BidCountProcessFun

// This factory creates AuctionWindowedRecordProcFun instances using AuctionGCounterWrapper
class BidCountWindowedFactory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
      new BidCountProcessFun(partition)
  }
}