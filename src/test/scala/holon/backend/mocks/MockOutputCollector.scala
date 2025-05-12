package holon.backend.mocks

import holon.LogProducer
import holon.LogProducerRecords
import holon.backend.OutputCollectorImpl

import scala.collection.mutable.Map

class MockOutputCollector(producers: Map[Byte, LogProducer]) extends OutputCollectorImpl(producers) {
    override def collect(chn: Byte, rec: LogProducerRecords): Unit = {
        // Do nothing
    }
}