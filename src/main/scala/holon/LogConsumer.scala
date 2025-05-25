package holon

import org.apache.kafka.common.{MetricName, Metric}

type LogConsumerRecords = Iterable[(Array[Byte], Array[Byte], Long)]

trait LogConsumer {
  def poll(): LogConsumerRecords
  def seek(partition: Int, offset: Long): Unit
  def metrics(): Map[MetricName,Metric]
  def lag(): Long
  def offsets(): Iterable[(Int, Long)]
  def close(): Unit
}
