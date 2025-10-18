package holon.example.nexmark

import holon.{ProcFun, ProcFunFactory}
import holon.backend.{Q0ProcessFun, WindowedRecordProcFunFullState}
import holon.crdt.PassThroughLWWMapWrapper
import holon.serialization.*

// this factory creates a Q0ProcessFun instance without any CRDTs
class Q0Factory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
    val crdts = List(WindowedRecordProcFunFullState(PassThroughLWWMapWrapper, partition, 0, gcounterRW))
    new Q0ProcessFun(partition, crdts)
  }
}