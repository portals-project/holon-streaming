package holon.utils

import org.apache.kafka.common.{MetricName, Metric}

type LogProducerRecords = Iterable[(Array[Byte], Array[Byte])]

trait LogProducer {
  def send(rec: LogProducerRecords): Unit
  def metrics(): Map[MetricName,Metric]
  def flush(): Unit
}
