package holon.example.nexmark

import holon.ProcFun
import holon.ProcFunFactory
import holon.backend.Q4ProcessFun

// This factory creates AuctionWindowedRecordProcFun instances using AuctionGCounterWrapper
class Q4Factory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
    new Q4ProcessFun(partition)
  }
}