package holon.example.nexmark.queryfactories

import holon.{ProcFun, ProcFunFactory}
import holon.backend.WindowedRecordProcFun
import holon.crdt.AuctionGCounterWrapper
import holon.example.nexmark.procfuns.Q5ProcessFun
import holon.serialization.mapGcounterRW

// This factory creates AuctionWindowedRecordProcFun instances using AuctionGCounterWrapper
class Q5Factory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
    val crdts = List(WindowedRecordProcFun(AuctionGCounterWrapper, partition, 0, mapGcounterRW))
    new Q5ProcessFun(partition, crdts)
  }
}