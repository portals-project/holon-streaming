package holon.examples.taxi

import holon.utils.*
import holon.core.Config.*
import holon.core.Utils.*
import holon.streaming.messaging.KafkaSystem
import holon.examples.taxi.HolonNode.*
import holon.examples.taxi.TaxiProducer.runProducer
import holon.examples.taxi.OutputConsumer.consumeOutput

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

        val system = holon.streaming.messaging.KafkaSystem(nrOfKafkaPartitions(), KAFKA_HOST, KAFKA_PORT)
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
                val holonInstance = holon.core.Holon(i)
                holonInstance.submitOrUpdate(j)
            }

            Thread.sleep(RUNTIME)
        }
    }
}
