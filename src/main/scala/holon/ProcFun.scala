package holon

abstract class ProcFun {
  def processInput(
      outputFunction: (Int, Byte, LogProducerRecords) => Unit,
      chn: Byte,
      rec: LogConsumerRecords,
  ): Unit

  def snapshot(): Array[Byte]

  def restore(snapshot: Array[Byte]): Unit
}
