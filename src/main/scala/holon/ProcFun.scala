package holon

// Is this where we call our modules?
abstract class ProcFun {
  def process(
      out: OutputCollector,
      chn: Byte,
      rec: LogConsumerRecords,
  ): Unit

  // Add snapshot, restore & broadcast functions
  // Function to take a snapshot of the current state
  def snapshot(): Array[Byte]

  // Function to restore the state from a snapshot
  def restore(snapshot: Array[Byte]): Unit
}
