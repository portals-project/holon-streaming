package holon

type LogConsumerRecords = Iterable[(Array[Byte], Array[Byte])]

trait LogConsumer {
  def poll(): LogConsumerRecords
  def seek(partition: Int, offset: Long): Unit
  def offsets(): Iterable[(Int, Long)]
  def close(): Unit
}
