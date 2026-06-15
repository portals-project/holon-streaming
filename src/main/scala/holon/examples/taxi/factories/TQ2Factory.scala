package holon.examples.taxi

import holon.streaming.processing.{ProcFun, ProcFunFactory}
import holon.examples.taxi.queries.TQ2ProcessFun
import holon.streaming.windowing.WindowedRecordProcFun
import holon.crdt.TQ2LWWMapWrapper
import holon.crdt.serialization.NexmarkQ7Serialization.lwwMapBytesRW

class TQ2Factory extends ProcFunFactory {
  override def create(partition: Int): ProcFun = {
    val crdts = List(WindowedRecordProcFun(TQ2LWWMapWrapper, partition, 0, lwwMapBytesRW))
    new TQ2ProcessFun(partition, crdts)
  }
}