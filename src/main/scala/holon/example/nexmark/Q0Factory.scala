package holon.example.nexmark

import holon.{ProcFun, ProcFunFactory}
import holon.backend.{Q0ProcessFun, WindowedRecordProcFunFullState}
import holon.crdt.PassThroughLWWMapWrapper
import holon.serialization.NexmarkQ0Serialization.gcounterRW

// this factory creates a Q0ProcessFun instance
class Q0Factory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
    val crdts = List(WindowedRecordProcFunFullState(PassThroughLWWMapWrapper, partition, 0, gcounterRW))
    new Q0ProcessFun(partition, crdts)
  }
}