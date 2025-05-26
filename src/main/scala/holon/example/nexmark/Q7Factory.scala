package holon.example.nexmark

import holon.ProcFun
import holon.ProcFunFactory
import holon.backend.WindowedRecordProcFun
import holon.crdt.HighestBidLWWRegisterWrapper
import holon.example.nexmark.procfuns.Q7ProcessFun
import holon.serialization.SerializationImplicits.lwwRegisterBytesRW

// This factory creates WindowedRecordProcFun's with a HighestBidLWWRegisterWrapper
class Q7Factory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
    val crdts = List(WindowedRecordProcFun(HighestBidLWWRegisterWrapper, partition, 0, lwwRegisterBytesRW)) 
    new Q7ProcessFun(partition, crdts)
  }
}