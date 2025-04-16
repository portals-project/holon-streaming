package holon.backend

import io.github.embeddedkafka.EmbeddedKafka
import io.github.embeddedkafka.EmbeddedKafkaConfig

class KafkaSystem(nPartitions: Int, host: String, port: Int):
  this.checkArguments()

  // INFO:
  // If there are issues with Kafka that seemingly some events do not arrive or
  // it is likely due to the topics not being set up correctly. For this,
  // set the line "auto.create.topics.enable" -> "true" in the config.

  private val customKafkaConfig: EmbeddedKafkaConfig = new EmbeddedKafkaConfig {
    override def kafkaPort = port
    override def zooKeeperPort = port + 1
    override def customBrokerProperties = Map(
      "auto.create.topics.enable" -> "false",
      "num.partitions" -> s"$nPartitions"
    )
    override def customProducerProperties: Map[String, String] = Map.empty
    override def customConsumerProperties: Map[String, String] = Map.empty
    override def numberOfThreads = nPartitions
  }

  EmbeddedKafka.start()(using customKafkaConfig)

  def startStream(topic: String): Unit =
    // Note: need to set partitions here, else it will not work :(.
    EmbeddedKafka.createCustomTopic(topic, partitions = nPartitions)(using customKafkaConfig)

  def stop(): Unit =
    EmbeddedKafka.stop()
    
  def deleteTopics(topics: List[String]): Unit =
    EmbeddedKafka.deleteTopics(topics)(using customKafkaConfig)

  private def checkArguments(): Unit =
    if nPartitions < 1 then throw new IllegalArgumentException("nPartitions must be at least 1")
    if host == null || host.isEmpty then throw new IllegalArgumentException("host must be non-empty")
