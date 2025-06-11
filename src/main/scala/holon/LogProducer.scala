package holon

type LogProducerRecords = Iterable[(Array[Byte], Array[Byte])]

trait LogProducer {
  def send(rec: LogProducerRecords): Unit
  def flush(): Unit
}
