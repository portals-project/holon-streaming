package holon.example.nexmark

import holon.ProcFun
import holon.ProcFunFactory
import holon.backend.Q7ProcessFun

// This factory creates AuctionWindowedRecordProcFun instances using AuctionGCounterWrapper
class Q7Factory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
    new Q7ProcessFun(partition)
  }
}