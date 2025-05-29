package holon.example.nexmark

import holon.ProcFun
import holon.ProcFunFactory
import holon.backend.{Q7ProcessFun, WindowedRecordProcFun}
import holon.crdt.HighestBidLWWMapWrapper
import holon.serialization.SerializationImplicits.lwwMapBytesRW

// This factory creates WindowedRecordProcFun's with a HighestBidLWWRegisterWrapper
class Q7Factory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
    val crdts = List(WindowedRecordProcFun(HighestBidLWWMapWrapper, partition, 0, lwwMapBytesRW))
    new Q7ProcessFun(partition, crdts)
  }
}