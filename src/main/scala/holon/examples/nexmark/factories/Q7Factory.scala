package holon.examples.nexmark.factories

import holon.streaming.processing.ProcFun
import holon.streaming.processing.ProcFunFactory
import holon.examples.nexmark.queries.Q7ProcessFun
import holon.streaming.windowing.WindowedRecordProcFun
import holon.crdt.HighestBidLWWRegisterWrapper
import holon.crdt.serialization.NexmarkQ7Serialization.lwwMapBytesRW

// this factory creates holon.streaming.windowing.WindowedRecordProcFun's with a HighestBidLWWRegisterWrapper
class Q7Factory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
    val crdts = List(WindowedRecordProcFun(HighestBidLWWRegisterWrapper, partition, 0, lwwMapBytesRW))
    new Q7ProcessFun(partition, crdts)
  }
}