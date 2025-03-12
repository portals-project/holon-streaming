package holon.example.nexmark

import upickle.default.*

import holon.*
import holon.backend.*
import holon.example.nexmark.Config.*
import holon.example.Nexmark
import holon.Utils.*

/** Count the total number of bids. */
object Query {
  Logger.setRootLevel("ERROR")

  // Define the consumer and producer references 
  private def consumerRef(chn: Byte, topic: String, partition: Int): ConsumerRef =
    ConsumerRef(
      chn = chn,
      host = KAFKA_HOST,
      port = KAFKA_PORT,
      topic = topic,
      partitions = List(partition),
    )

  private def producerRef(chn: Byte, topic: String): ProducerRef =
    ProducerRef(
      chn = chn,
      host = KAFKA_HOST,
      port = KAFKA_PORT,
      topic = topic,
    )

  // Job function creates a job object with the specified consumers and producers
  def job(partitions: List[Int]): Job = {
    val consumers = partitions.map { partition =>
      consumerRef(CHN_NEXMARK, KAFKA_TOPIC_NEXMARK, partition)
    } :+ consumerRef(CHN_BROADCAST, KAFKA_TOPIC_BROADCAST, 0)

    val producers = List(
      producerRef(CHN_NEXMARK, KAFKA_TOPIC_NEXMARK),
      producerRef(CHN_BROADCAST, KAFKA_TOPIC_BROADCAST),
      producerRef(CHN_OUTPUT, KAFKA_TOPIC_OUTPUT),
    )

    val job = Job(
      consumers = consumers,
      producers = producers,
      procFunFactory = new RecordProcFunFactory(),
      partitions = partitions,
    )

    job
  }

  /** Run the Nexmark producer */
  def runNexmarkProducer() = {
    val producer = KafkaLogProducer(KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC_NEXMARK)
    val iter = Nexmark.iterator()
    val logger = Logger.apply("Producer")
    Logger.setLevel("Producer", "INFO")
    logger.info("Starting Nexmark Producer")
    while true do
      for i <- 0 until KAFKA_N_PARTITIONS do
        val batch = (0 until PRODUCER_BATCH_SIZE).map(_ => iter.next()).map(x => (writeBinary(i), Nexmark.serialize(x)))
        println(s"Sending batch (size ${batch.size})")
        producer.send(batch)

      producer.flush()
      Thread.sleep(PRODUCER_SLEEP_MS)
  }

  def runOutputConsumer() = {
    val logger = Logger.apply("Consumer")
    Logger.setLevel("Consumer", "INFO")
    logger.info("Starting Output Consumer")
    val output = KafkaLogConsumer(KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC_OUTPUT, (0 until KAFKA_N_PARTITIONS).toList)
    while true do
      output.poll() match
        case Nil =>
          Thread.sleep(CONSUMER_SLEEP_MS)
        case records =>
          records.foreach: r =>
            val partition = readBinary[Int](r._1)
            logger.info(s"[OUTPUT TOPIC]: partition: $partition, received record: $r")
//            val outputState = readBinary[OutputState](r._2)
//            logger.info(s"[OUTPUT TOPIC]: partition: $partition, window: ${outputState.window} closed with final aggregate: ${outputState.value}")
  }

  def setupKafka(): Unit = {
    val logger = Logger.apply("Kafka")
    Logger.setLevel("Kafka", "INFO")
    logger.info("Setting up Kafka")

    val system = KafkaSystem(KAFKA_N_PARTITIONS, KAFKA_HOST, KAFKA_PORT)
    system.startStream(KAFKA_TOPIC_NEXMARK)
    system.startStream(KAFKA_TOPIC_BROADCAST)
    system.startStream(KAFKA_TOPIC_OUTPUT)

    logger.info("Kafka setup complete")
  }

  def main(args: Array[String]): Unit = {
    val RUNTIME = 25_000
    SafeRun(RUNTIME) {
      val logger = Logger.apply("Nexmark Query")
      Logger.setLevel("Nexmark Query", "INFO")
      logger.info("Starting Nexmark Query")
      setupKafka()

      RunThread(runNexmarkProducer())
      RunThread(runNexmarkProducer())
      RunThread(runNexmarkProducer())
      RunThread(runNexmarkProducer())
      RunThread(runNexmarkProducer())
      RunThread(runOutputConsumer())

      for (i <- 0 until N_NODES) {
        val partitions = (i * PARTITIONS_PER_NODE until (i + 1) * PARTITIONS_PER_NODE).toList
        System.out.println(s" Node: $i Partitions: $partitions")
        val j = job(partitions)
        val holon = Holon(i)
        holon.submitOrUpdate(j)
      }

      Thread.sleep(RUNTIME)
    }
  }
}
