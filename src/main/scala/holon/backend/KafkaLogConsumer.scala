package holon.backend

import holon._
import holon.backend.KafkaSerdes._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common._

import java.util.Properties
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.mutable
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

    private val logger = Logger.apply("KafkaLogConsumer")
    private val outputLog = org.slf4j.LoggerFactory.getLogger("com.holon.system.output")
    Logger.setLevel("KafkaLogConsumer", "INFO")

    // Internal buffer for holding polled records
    private val buffer = new ConcurrentLinkedQueue[(Array[Byte], Array[Byte], Long)]()

    // Ramp parameters
    private val stepDurationMs = 10_000L // 10s
    private val startBatchSize = 500
    private val batchIncrement = 100
    private val maxBatchSize = 1_000_000 // 1 million
    private val initWait = 90_000L // 1.5s initial wait before ramping up

    private var currentBatchSize = startBatchSize
    private var nextRampTime = System.currentTimeMillis() + initWait

    override def poll(): LogConsumerRecords = {
        // Check if buffer needs refill
//        if (buffer.size() < maxBatchSize / 2) {
//            val newRecords = cons.poll(java.time.Duration.ofMillis(50)).asScala.map { rec =>
//                (rec.key(), rec.value(), rec.timestamp())
//            }
//            newRecords.foreach(buffer.add)
//        }
//
//        // Apply timed ramp-up logic
//        val now = System.currentTimeMillis()
//        if (now >= nextRampTime && currentBatchSize < maxBatchSize) {
//            currentBatchSize = (currentBatchSize + batchIncrement).min(maxBatchSize)
//            nextRampTime = now + stepDurationMs
////            logger.info(s"[RampBuffer] Increased batch size to $currentBatchSize, next ramp in ${stepDurationMs} ms, current time: $now")
//            outputLog.info(s"[RampBuffer] Increased batch size to $currentBatchSize, next ramp in ${stepDurationMs} ms, current time: $now")
//        }
//
//        // Drain up to currentBatchSize from the buffer
//        val result = mutable.ListBuffer.empty[(Array[Byte], Array[Byte], Long)]
//        var i = 0
//        while (i < currentBatchSize && !buffer.isEmpty) {
//            val record = buffer.poll()
//            if (record != null) {
//                result += record
//                i += 1
//            }
//        }
//
//        result.toList

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
        s"KafkaLogConsumer(host=$host, port=$port, topic=$topic, partitions=$partitions)"
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
