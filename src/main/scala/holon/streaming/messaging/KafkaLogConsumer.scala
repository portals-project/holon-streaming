package holon.streaming.messaging

import holon.utils.{Logger, LogConsumerRecords, LogConsumer}
import holon.core.{ConsumerRef}
import holon.streaming.messaging.KafkaSerdes._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common._

import java.util.Properties
import scala.jdk.CollectionConverters._

class KafkaLogConsumer(
                        host: String,
                        port: Int,
                        topic: String,
                        partitions: List[Int],
                      ) extends LogConsumer {

    private val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":" + port)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "holon-consumers")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KAFKA_DES)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KAFKA_DES)
//    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10000") // 10k
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500") // 500
    private val cons = new KafkaConsumer[Array[Byte], Array[Byte]](props)
    cons.assign(partitions.map(p => new TopicPartition(topic, p)).asJava)
    val partition: Int = if (partitions.nonEmpty) partitions.head else -1

    Logger.setLevel("holon.streaming.messaging.KafkaLogConsumer", "INFO")

    override def poll(): LogConsumerRecords = {
        val records = cons.poll(java.time.Duration.ZERO).asScala.map { rec =>
            (rec.key(), rec.value(), rec.timestamp())
        }.toList

        records
    }

    override def seek(partition: Int, offset: Long): Unit =
        cons.seek(new TopicPartition(topic, partition), offset)

    override def metrics(): scala.collection.immutable.Map[MetricName, Metric] =
        cons.metrics().asScala.toMap.view.toMap

    override def lag(): Long =
        cons.currentLag(new TopicPartition(topic, partition)).orElse(0L)

    override def offsets(): Iterable[(Int, Long)] =
        cons.assignment().asScala.map { tp =>
            (tp.partition(), cons.position(tp))
        }

    override def close(): Unit = cons.close()

    override def toString: String =
        s"holon.streaming.messaging.KafkaLogConsumer(host=$host, port=$port, topic=$topic, partitions=$partitions)"
}

object KafkaLogConsumer {
    def fromRef(ref: ConsumerRef): KafkaLogConsumer =
        new KafkaLogConsumer(
            ref.host,
            ref.port,
            ref.topic,
            ref.partitions,
        )
}