package holon

abstract class ProcFun {
  def processInput(
      outputFunction: (Int, Byte, LogProducerRecords) => Unit,
      chn: Byte,
      rec: LogConsumerRecords,
  ): Unit

  // function to take a snapshot of the current state
  def snapshot(): Array[Byte]

  // function to restore the state from a snapshot
  def restore(snapshot: Array[Byte]): Unit
}
