package holon.example.nexmark

import holon.{ProcFun, ProcFunFactory}
import holon.backend.{Q0ProcessFun, Q7ProcessFun, WindowedRecordProcFun}
import holon.crdt.HighestBidLWWRegisterWrapper
import holon.serialization.SerializationImplicits.lwwRegisterBytesRW

// This factory creates a Q0ProcessFun instance without any CRDTs
class Q0Factory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = { 
    new Q0ProcessFun(partition)
  }
}