package holon.example.taxi

import holon.backend.WindowedRecordProcFun
import holon.crdt.HighestBidLWWRegisterWrapper
import holon.serialization.SerializationImplicits.lwwRegisterBytesRW
import holon.{ProcFun, ProcFunFactory}

// This factory creates a Q0ProcessFun instance without any CRDTs
class Q2Factory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = { 
    new Q2ProcessFun(partition)
  }
}