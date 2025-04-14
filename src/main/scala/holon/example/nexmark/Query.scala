package holon.example.nexmark

import holon.*
import holon.Utils.*
import holon.backend.*
import holon.example.Nexmark
import holon.example.nexmark.HolonNode.*
import Config.*
import upickle.default.*

/** Count the total number of bids. */
object Query {
    Logger.setRootLevel("ERROR")

    /** Run the Nexmark producer */
    def runNexmarkProducer() = {
        val producer = KafkaLogProducer(KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC_INPUT)
        val iter = Nexmark.iterator()
        val logger = Logger.apply("Producer")
        Logger.setLevel("Producer", "INFO")
        logger.info("Starting Nexmark Producer")
        while true do
            for i <- 0 until nrOfKafkaPartitions() do
                val batch = (0 until PRODUCER_BATCH_SIZE).map(_ => iter.next()).map(x => (writeBinary(i), Nexmark.serialize(x)))
                // println(s"Sending batch (size ${batch.size})")
                producer.send(batch)

            producer.flush()
            Thread.sleep(PRODUCER_SLEEP_MS)
    }

    def runOutputConsumer() = {
        val logger = Logger.apply("Consumer")
        Logger.setLevel("Consumer", "INFO")
        logger.info("Starting Output Consumer")
        val output = KafkaLogConsumer(KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC_OUTPUT, (0 until nrOfKafkaPartitions()).toList)
        while true do
            output.poll() match
                case Nil =>
                    Thread.sleep(CONSUMER_SLEEP_MS)
                case records =>
                    records.foreach: r =>
                        val outputState = readBinary[OutputState](r._2)
                        // Deconstruct the output state
                        val partition = outputState.partition
                        val windowId = outputState.window
                        val auctionId = outputState.auctionId
                        val crdtValue = outputState.bidCount
                        
                        logger.info(s"[OUTPUT]: partition: $partition window: $windowId auction: $auctionId closed a window with final aggregate: $crdtValue")
    }

    def setupKafka(): Unit = {
        val logger = Logger.apply("Kafka")
        Logger.setLevel("Kafka", "INFO")
        logger.info("Setting up Kafka")

        val system = KafkaSystem(nrOfKafkaPartitions(), KAFKA_HOST, KAFKA_PORT)
        system.startStream(KAFKA_TOPIC_INPUT)
        system.startStream(KAFKA_TOPIC_BROADCAST)
        system.startStream(KAFKA_TOPIC_CONTROL)
        system.startStream(KAFKA_TOPIC_OUTPUT)

        logger.info("Kafka setup complete")
    }

    def main(args: Array[String]): Unit = {
        val RUNTIME = 45_000
        SafeRun(RUNTIME) {
            val logger = Logger.apply("Nexmark Query")
            Logger.setLevel("Nexmark Query", "INFO")
            logger.info("Starting Nexmark Query")
            setupKafka()

//            RunThread(runNexmarkProducer())
//            RunThread(runNexmarkProducer())
//            RunThread(runNexmarkProducer())
//            RunThread(runNexmarkProducer())
            RunThread(runNexmarkProducer())
            RunThread(runOutputConsumer())

            for (i <- 0 until N_NODES) {
                val partitions = (i * PARTITIONS_PER_NODE until (i + 1) * PARTITIONS_PER_NODE).toList
                val j = job(partitions, KAFKA_HOST, KAFKA_PORT)
                val holon = Holon(i)
                holon.submitOrUpdate(j)
            }

            Thread.sleep(RUNTIME)
        }
    }
}
