package holon.streaming.output

import scala.collection.mutable.Map

import holon.utils.*

class OutputCollectorImpl(producers: Map[Byte, LogProducer]) extends OutputCollector {
  def collect(chn: Byte, rec: LogProducerRecords): Unit = {
    producers.get(chn) match {
      case Some(producer) => producer.send(rec)
      case None =>
        // Log warning but don't crash if producer is not available yet
        println(s"Warning: Producer for channel $chn not available yet")
    }
  }
}
