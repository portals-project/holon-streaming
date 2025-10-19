package holon.example.taxi

import holon.{ProcFun, ProcFunFactory}
import holon.backend.{TQ2ProcessFun, WindowedRecordProcFun}
import holon.crdt.TQ2LWWMapWrapper
import holon.serialization.NexmarkQ7Serialization.lwwMapBytesRW

class TQ2Factory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
    val crdts = List(WindowedRecordProcFun(TQ2LWWMapWrapper, partition, 0, lwwMapBytesRW))
    new TQ2ProcessFun(partition, crdts)
  }
}