package holon.example.nexmark

import holon.ProcFun
import holon.ProcFunFactory
import holon.backend.Q5ProcessFun

// This factory creates AuctionWindowedRecordProcFun instances using AuctionGCounterWrapper
class Q5Factory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
    new Q5ProcessFun(partition)
  }
}