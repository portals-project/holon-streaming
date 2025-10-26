package holon.streaming.messaging

import java.util.Properties

import org.apache.kafka.clients.producer.*

import holon.utils.*
import holon.core.ProducerRef
import holon.streaming.messaging.KafkaSerdes.*
import org.apache.kafka.common.{MetricName, Metric}
import scala.jdk.CollectionConverters._

class KafkaLogProducer(
    host: String,
    port: Int,
    topic: String,
) extends LogProducer {
  private val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":" + port)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KAFKA_SER)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KAFKA_SER)
  props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "holon.backend.IdentityPartitioner")
  // KafkaProducer: A Kafka client that publishes records to the Kafka cluster. The producer is thread safe and
  // sharing a single producer instance across threads will generally be faster than having multiple instances.
  private val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)

  def send(recs: LogProducerRecords): Unit =
    for (rec <- recs) {
      // A producer record is what will be sent to the Kafka cluster
      val record = new ProducerRecord(topic, rec._1, rec._2)
      producer.send(record)
    }

  // TODO: Change this to something simpler
  override def metrics(): scala.collection.immutable.Map[MetricName,Metric] = {
    producer.metrics().asScala.toMap.view.toMap
  }

  def flush(): Unit =
    producer.flush()
}

object KafkaLogProducer {
  def fromRef(ref: ProducerRef): KafkaLogProducer =
    new KafkaLogProducer(
      ref.host,
      ref.port,
      ref.topic,
    )
}
