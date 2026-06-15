package holon.examples.nexmark.factories

import holon.streaming.processing.ProcFun
import holon.streaming.processing.ProcFunFactory
import holon.examples.nexmark.queries.Q4ProcessFun
import holon.streaming.windowing.WindowedRecordProcFun
import holon.crdt.{AuctionToCategoryWrapper, AuctionToHighestBidWrapper}
import holon.crdt.serialization.NexmarkQ4Serialization.rwTuple

// this factory creates holon.streaming.windowing.WindowedRecordProcFun's with a AuctionToHighestBidWrapper and a AuctionToCategoryWrapper
class Q4Factory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
    val crdts = List(
      WindowedRecordProcFun(AuctionToHighestBidWrapper, partition, 0, rwTuple),
      WindowedRecordProcFun(AuctionToCategoryWrapper, partition, 1, rwTuple)
    )
    new Q4ProcessFun(partition, crdts)
  }
}