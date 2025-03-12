package holon

// Is this where we call our modules?
abstract class ProcFun {
  def process(
      outputFunction: (Int, Byte, LogProducerRecords) => Unit,
      chn: Byte,
      rec: LogConsumerRecords,
  ): Unit
  
  // Function to define the window for the incoming event
  def defineWindow(eventTime: Long): Long

  // Function to take a snapshot of the current state
  def snapshot(): Array[Byte]

  // Function to restore the state from a snapshot
  def restore(snapshot: Array[Byte]): Unit
}
