package holon.example.nexmark

import holon.ProcFun
import holon.ProcFunFactory
import holon.backend.{Q4ProcessFun, WindowedRecordProcFun}
import holon.crdt.AuctionGCounterWrapper
import org.apache.pekko.cluster.ddata.GCounter

// This factory creates AuctionWindowedRecordProcFun instances using AuctionGCounterWrapper
class Q4Factory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
    new Q4ProcessFun(partition)
  }
}