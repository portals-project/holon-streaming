package holon.utils

import org.apache.kafka.common.{MetricName, Metric}

trait LogConsumer {
  def poll(): LogConsumerRecords
  def seek(partition: Int, offset: Long): Unit
  def metrics(): Map[MetricName,Metric]
  def lag(): Long
  def offsets(): Iterable[(Int, Long)]
  def close(): Unit
}
