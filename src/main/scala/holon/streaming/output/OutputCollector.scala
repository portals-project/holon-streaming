package holon.streaming.output

import holon.utils.LogProducerRecords

trait OutputCollector {
  def collect(chn: Byte, rec: LogProducerRecords): Unit
}
