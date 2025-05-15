package holon.example.nexmark

import holon.ProcFun
import holon.ProcFunFactory
import holon.backend.{Q4ProcessFun, WindowedRecordProcFun}
import holon.crdt.{AuctionToCategoryWrapper, AuctionToHighestBidWrapper, HighestBidLWWRegisterWrapper}
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