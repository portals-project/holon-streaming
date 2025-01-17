package holon

abstract class ProcFun {
  def process(
      out: OutputCollector,
      chn: Byte,
      rec: LogConsumerRecords,
  ): Unit
}
