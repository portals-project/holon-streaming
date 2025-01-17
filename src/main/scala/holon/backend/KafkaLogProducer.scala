package holon.backend

import java.util.Properties

import org.apache.kafka.clients.producer.*

import holon.*
import holon.backend.KafkaSerdes.*

class KafkaLogProducer(
    host: String,
    port: Int,
    topic: String,
) extends LogProducer {

  // setup
  private val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":" + port)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KAFKA_SER)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KAFKA_SER)
  props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "holon.backend.IdentityPartitioner")
  private val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)

  def send(recs: LogProducerRecords): Unit =
    for (rec <- recs) {
      val record = new ProducerRecord(topic, rec._1, rec._2)
      producer.send(record)
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
