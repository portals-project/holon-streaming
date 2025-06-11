package holon.example.nexmark.queryfactories

import holon.backend.WindowedRecordProcFun
import holon.crdt.HighestBidLWWRegisterWrapper
import holon.example.nexmark.procfuns.{Q0ProcessFun, Q7ProcessFun}
import holon.serialization.SerializationImplicits.lwwRegisterBytesRW
import holon.{ProcFun, ProcFunFactory}

// This factory creates a Q0ProcessFun instance without any CRDTs
class Q0Factory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = { 
    new Q0ProcessFun(partition)
  }
}