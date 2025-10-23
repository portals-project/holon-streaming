package holon.example.nexmark

import holon.*
import holon.Utils.*
import holon.backend.*
import holon.example.nexmark.HolonNode.*
import Config.*
import holon.example.nexmark.OutputConsumer.consumeOutput
import holon.example.nexmark.NexmarkProducer.runProducer

object Query {
    Logger.setRootLevel("ERROR")

    // run Nexmark producer
    def runNexmarkProducer(): Unit = {
        runProducer(KAFKA_HOST, KAFKA_PORT, PRODUCER_SLEEP_MS)
    }

    // run Output Consumer
    def runOutputConsumer(): Unit = {
        val logger = Logger.apply("Consumer")
        Logger.setLevel("Consumer", "INFO")
        logger.info("Starting Output Consumer")

        consumeOutput(KAFKA_HOST, KAFKA_PORT)
    }

    // setup Kafka
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
        val RUNTIME = 300_000
        SafeRun(RUNTIME) {
            val logger = Logger.apply("Nexmark Query")
            Logger.setLevel("Nexmark Query", "INFO")
            logger.info("Starting Nexmark Query")
            setupKafka()

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
