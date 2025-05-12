package holon.backend

import holon.Config.{KAFKA_HOST, KAFKA_PORT}
import holon.Logger

import java.time.Duration
import java.util.Properties
import org.apache.kafka.clients.admin.{AdminClient, ListTopicsOptions}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import scala.jdk.CollectionConverters.*

object KafkaDebugger extends App {
  private val logger = Logger("KafkaDebugger")
  Logger.setLevel("KafkaDebugger", "INFO")
  logger.info("Starting KafkaDebugger")

  /** Admin client config & topic lister */
  private def setupAdminConfig(): Properties = {
    val bootstrap = s"$KAFKA_HOST:$KAFKA_PORT"
    val props = new Properties()
    props.put("bootstrap.servers", bootstrap)
    props.put("request.timeout.ms", "5000")
    props
  }

  def listTopics(): Unit = {
    val admin = AdminClient.create(setupAdminConfig())
    try {
      val namesFuture = admin
        .listTopics(new ListTopicsOptions().timeoutMs(5000))
        .names()
      val topics = namesFuture.get()
      logger.info(s"[KAFKA-DEBUGGER] Found ${topics.size()} topics:")
      topics.asScala.toSeq.sorted.foreach(t => logger.info(s"[KAFKA-DEBUGGER]  • $t"))
    } finally admin.close()
  }

  /**
   * Fetches and prints all messages from `topic` whose partition id satisfies `partitionFilter`.
   *
   * @param topic            the topic name
   * @param partitionFilter  a predicate on partition id (e.g. _ == 2, _ >= 1 && _ <= 3, etc.)
   * @param pollTimeoutMs    how long to wait on each poll (default 1s)
   * @param maxEmptyPolls    after this many consecutive empty polls, stop consuming (default 5)
   */
  def listMessagesByPartition(
                               topic: String,
                               partitionFilter: Int => Boolean,
                               pollTimeoutMs: Long = 1000L,
                               maxEmptyPolls: Int = 5
                             ): Unit = {
    val bootstrap = s"$KAFKA_HOST:$KAFKA_PORT"
    val props = new Properties()
    props.put("bootstrap.servers", bootstrap)
    props.put("group.id", "kafka-debugger-consumer")
    props.put("key.deserializer", classOf[StringDeserializer].getName)
    props.put("value.deserializer", classOf[StringDeserializer].getName)
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "false")

    val consumer = new KafkaConsumer[String, String](props)

    try {
      val allPartitions = consumer
        .partitionsFor(topic)
        .asScala
        .map(info => new TopicPartition(topic, info.partition()))
        .filter(tp => partitionFilter(tp.partition()))
        .toList

      if (allPartitions.isEmpty) {
        logger.warn(s"[KAFKA-DEBUGGER] No partitions of '$topic' match your filter.")
        return
      }

      consumer.assign(allPartitions.asJava)
      logger.info(s"[KAFKA-DEBUGGER] Assigned to partitions: ${allPartitions.map(_.partition()).mkString(", ")}")

      var emptyPolls = 0
      while (emptyPolls < maxEmptyPolls) {
        val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(pollTimeoutMs))
        if (records.isEmpty) {
          emptyPolls += 1
        } else {
          emptyPolls = 0
          records.asScala.foreach { record =>
            logger.info(
              s"[KAFKA-DEBUGGER] topic=${record.topic()} partition=${record.partition()} " +
                s"offset=${record.offset()} key=${record.key()} value=${record.value()}"
            )
          }
        }
      }
      logger.info(s"[KAFKA-DEBUGGER] No more messages after $maxEmptyPolls empty polls, stopping.")
    } finally {
      consumer.close()
    }
  }

  // Example usage:
  // listTopics()
  // listMessagesByPartition("broadcast", _ == 2)
}
