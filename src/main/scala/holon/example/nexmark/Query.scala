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
  def job(partition: Int): Job = {
    val consumers = List(
      consumerRef(CHN_NEXMARK, KAFKA_TOPIC_NEXMARK, partition),
      consumerRef(CHN_BROADCAST, KAFKA_TOPIC_BROADCAST, 0),
    )

    val producers = List(
      producerRef(CHN_NEXMARK, KAFKA_TOPIC_NEXMARK),
      producerRef(CHN_BROADCAST, KAFKA_TOPIC_BROADCAST),
      producerRef(CHN_OUTPUT, KAFKA_TOPIC_OUTPUT),
    )

    val job = Job(
      consumers = consumers,
      producers = producers,
      procFun = new RecordProcFun(partition),
    )

    job
  }

  /** Run the Nexmark producer */
  def runNexmarkProducer() = {
    val producer = KafkaLogProducer(KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC_NEXMARK)
    val iter = Nexmark.iterator()
    while true do
      for i <- 0 until KAFKA_N_PARTITIONS do //
        // TODO: Add extra bit for work stealing
        val batch = (0 until PRODUCER_BATCH_SIZE).map(_ => iter.next()).map(x => (writeBinary(i), Nexmark.serialize(x)))
        producer.send(batch)
      producer.flush()
      Thread.sleep(PRODUCER_SLEEP_MS)
  }

  def runOutputConsumer() = {
    val logger = Logger.apply("Consumer")
    Logger.setLevel("Consumer", "INFO")
    val output = KafkaLogConsumer(KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC_OUTPUT, (0 until KAFKA_N_PARTITIONS).toList)
    while true do
      output.poll() match
        case Nil =>
          Thread.sleep(CONSUMER_SLEEP_MS)
        case records =>
          records.foreach: r =>
            val bids = readBinary[(Long)](r._2)
            logger.info(s"Bids: $bids")
  }

  def setupKafka(): Unit = {
    val system = KafkaSystem(KAFKA_N_PARTITIONS, KAFKA_HOST, KAFKA_PORT)
    system.startStream(KAFKA_TOPIC_NEXMARK)
    system.startStream(KAFKA_TOPIC_BROADCAST)
    system.startStream(KAFKA_TOPIC_OUTPUT)
  }

  def main(args: Array[String]): Unit = {
    SafeRun(10_000) {
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

      for (i <- 0 until KAFKA_N_PARTITIONS) {
        val j = job(i)
        val holon = Holon(i)
        holon.submitOrUpdate(j)
      }

      Thread.sleep(10_000)
    }
  }
}
