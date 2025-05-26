package holon.example.nexmark

import holon.ProcFun
import holon.ProcFunFactory
import holon.backend.WindowedRecordProcFun
import holon.crdt.BidCountGCounterWrapper
import holon.example.nexmark.procfuns.BidCountProcessFun
import holon.serialization.*

// This factory creates AuctionWindowedRecordProcFun instances using AuctionGCounterWrapper
class BidCountWindowedFactory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
      val crdts = List(WindowedRecordProcFun(BidCountGCounterWrapper, partition, 0, gcounterRW))
      new BidCountProcessFun(partition, crdts)
  }
}