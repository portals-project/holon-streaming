package holon.example.nexmark

import holon.ProcFun
import holon.ProcFunFactory
import holon.backend.{Q5ProcessFun, WindowedRecordProcFun}
import holon.crdt.AuctionGCounterWrapper
import holon.serialization.mapGcounterRW

// This factory creates AuctionWindowedRecordProcFun instances using AuctionGCounterWrapper
class Q5Factory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
    val crdts = List(WindowedRecordProcFun(AuctionGCounterWrapper, partition, 0, mapGcounterRW))
    new Q5ProcessFun(partition, crdts)
  }
}