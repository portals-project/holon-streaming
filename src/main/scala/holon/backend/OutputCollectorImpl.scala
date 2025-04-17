package holon.backend

import scala.collection.mutable.Map

import holon.*

class OutputCollectorImpl(producers: Map[Byte, LogProducer]) extends OutputCollector {
  def collect(chn: Byte, rec: LogProducerRecords): Unit =
    if (producers.contains(chn))
      producers(chn).send(rec)
}
