package holon.example.taxi

import holon.*
import holon.Config.*
import holon.Utils.*
import holon.backend.*
import holon.backend.KafkaSystem
import holon.example.taxi.HolonNode.*
import holon.example.taxi.TaxiProducer.runProducer
import holon.example.taxi.OutputConsumer.consumeOutput
import upickle.legacy.*

/** Count the total number of bids. */
object TaxiQuery {
    Logger.setRootLevel("ERROR")

    /** Run the Taxi producer */
    def runTaxiDataProducer() = {
        runProducer(KAFKA_HOST, KAFKA_PORT, PRODUCER_SLEEP_MS)
    }

    def runOutputConsumer() = {
        val logger = Logger.apply("Consumer")
        Logger.setLevel("Consumer", "INFO")
        logger.info("Starting Output Consumer")

        consumeOutput(KAFKA_HOST, KAFKA_PORT)
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
        val RUNTIME = 100_000
        SafeRun(RUNTIME) {
            val logger = Logger.apply("Taxi Query")
            Logger.setLevel("Taxi Query", "INFO")
            logger.info("Starting Taxi Query")
            setupKafka()

            RunThread(runTaxiDataProducer())
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
