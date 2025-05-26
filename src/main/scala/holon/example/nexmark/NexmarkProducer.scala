package holon.example.nexmark

import holon.*
import holon.backend.*
import holon.example.Nexmark
import Config.*
import holon.backend.kafka.KafkaLogProducer
import upickle.default.*

import scala.collection.mutable

object NexmarkProducer {
    private val FirestoreClient = holon.backend.cloud.FirestoreClient

    private val logger = Logger("NexmarkProducer")
    Logger.setLevel("NexmarkProducer", "INFO")

    def main(args: Array[String]): Unit = {
        setupConfig()

        val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
        val host = kafkaBootstrapServers.split(":").head
        val port = kafkaBootstrapServers.split(":").last.toInt
        val PRODUCER_SLEEP_TIME_MS = sys.env.getOrElse("PRODUCER_SLEEP_TIME_MS", "100").toInt

        while (!FirestoreClient.isStartFlagSet) {
            logger.info("Waiting for start flag to be set.")
            Thread.sleep(1_000)
        }
        Thread.sleep(2_000) // Let holon nodes start first

        runProducer(host, port, PRODUCER_SLEEP_TIME_MS)
    }

    def runProducer(kafkaHost: String, kafkaPort: Int, sleepTime: Int): Unit = {
        val producer = KafkaLogProducer(kafkaHost, kafkaPort, KAFKA_TOPIC_INPUT)
        val iter = Nexmark.iterator()

        // TODO: Only used for benchmarking.
        // Track the amount of input events per window (even if they are not the specific CRDT class).
        val inputEventsPerWindow = mutable.Map.empty[Long, Long]

        while true do
            for i <- 0 until nrOfKafkaPartitions() do
                val batch = (0 until PRODUCER_BATCH_SIZE)
                    .map(_ => iter.next())
                    .map(x => {
                        val window = defineWindow(x.timestamp)
                        inputEventsPerWindow(window) = inputEventsPerWindow.getOrElse(window, 0L) + 1
                        (writeBinary(i), Nexmark.serialize(x))
                    })
                producer.send(batch)

            producer.flush()

            // Get max key from inputEventsPerWindow
            val currentMaxWindow = inputEventsPerWindow.keys.max
            for ((k, v) <- inputEventsPerWindow) {
                if (k < currentMaxWindow) {
                    logger.info(s"[Throughput] window: $k, eventCount: ${inputEventsPerWindow.getOrElse(k, 0)}")
                    inputEventsPerWindow -= k
                }
            }

            Thread.sleep(sleepTime)
    }

    def setupConfig(): Unit = {
        val N_NODES = sys.env.getOrElse("N_NODES", "2").toInt
        Config.N_NODES = N_NODES
        val PARTITIONS_PER_NODE = sys.env.getOrElse("PARTITIONS_PER_NODE", "2").toInt
        Config.PARTITIONS_PER_NODE = PARTITIONS_PER_NODE
        val WINDOW_L = sys.env.getOrElse("WINDOW_LENGTH", "10000").toLong
        Config.WINDOW_LENGTH = WINDOW_L
    }

    def defineWindow(eventTime: Long): Long = {
        val time = eventTime / 10
        if (time % WINDOW_LENGTH == 0) time / WINDOW_LENGTH else (time / WINDOW_LENGTH) + 1
    }
}

