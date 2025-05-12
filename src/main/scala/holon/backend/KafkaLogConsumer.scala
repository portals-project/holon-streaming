package holon.backend

import holon.*
import holon.backend.KafkaSerdes.*
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.*

import java.util.Properties
import scala.jdk.CollectionConverters.*

class KafkaLogConsumer(
    host: String,
    port: Int,
    topic: String,
    partitions: List[Int],
) extends LogConsumer {

    // setup
    private val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":" + port)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "holon-consumers")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KAFKA_DES)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KAFKA_DES)
    private val cons = new KafkaConsumer[Array[Byte], Array[Byte]](props)
    cons.assign(partitions.map(p => new TopicPartition(topic, p)).asJava)
    val partition: Int = if (partitions.nonEmpty) partitions.head else -1 // TODO: allow for multiple partitions per consumer

    override def poll(): LogConsumerRecords =
        val records = cons.poll(java.time.Duration.ZERO)
        records.asScala.map { rec =>
            (rec.key(), rec.value(), rec.timestamp())
        }

    override def seek(partition: Int, offset: Long): Unit =
        cons.seek(new TopicPartition(topic, partition), offset)

    override def lag(): Long =
        cons.currentLag(new TopicPartition(topic, partition)).orElse(0L)

    override def offsets(): Iterable[(Int, Long)] =
        cons.assignment().asScala.map { tp =>
            (tp.partition(), cons.position(tp))
        }

    override def close(): Unit = {
        cons.close()
    }

    override def toString: String = {
        s"KafkaLogConsumer(host=$host, port=$port, topic=$topic, partitions=$partitions)"
    }
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
