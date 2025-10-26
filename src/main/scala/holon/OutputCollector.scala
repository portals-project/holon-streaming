package holon

trait OutputCollector {
  def collect(chn: Byte, rec: LogProducerRecords): Unit
}
