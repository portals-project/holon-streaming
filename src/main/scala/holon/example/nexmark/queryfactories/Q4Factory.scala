package holon.example.nexmark.queryfactories

import holon.{ProcFun, ProcFunFactory}
import holon.backend.WindowedRecordProcFun
import holon.crdt.{AuctionToCategoryWrapper, AuctionToHighestBidWrapper}
import holon.example.nexmark.procfuns.Q4ProcessFun
import holon.serialization.*

// This factory creates WindowedRecordProcFun's with a AuctionToHighestBidWrapper and a AuctionToCategoryWrapper
class Q4Factory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
    val crdts = List(
      WindowedRecordProcFun(AuctionToHighestBidWrapper, partition, 0, rwLWWMap),
      WindowedRecordProcFun(AuctionToCategoryWrapper, partition, 1, rwTuple)
    )
    new Q4ProcessFun(partition, crdts)
  }
}