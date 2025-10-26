package holon.examples.nexmark

import holon.streaming.processing.{ProcFun, ProcFunFactory}
import holon.examples.nexmark.queries.Q0ProcessFun
import holon.streaming.windowing.WindowedRecordProcFunFullState
import holon.crdt.PassThroughLWWMapWrapper
import holon.crdt.serialization.NexmarkQ0Serialization.gcounterRW

// this factory creates a Q0ProcessFun instance
class Q0Factory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
    val crdts = List(WindowedRecordProcFunFullState(PassThroughLWWMapWrapper, partition, 0, gcounterRW))
    new Q0ProcessFun(partition, crdts)
  }
}