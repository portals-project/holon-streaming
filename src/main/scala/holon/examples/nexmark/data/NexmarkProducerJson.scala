package holon.examples.nexmark.data

import holon.utils.*
import holon.core.Config.*
import org.slf4j.LoggerFactory
import upickle.default.*

import scala.collection.mutable

object NexmarkProducerJson {
    private val logger = Logger("NexmarkProducer")
    private val outputLog = LoggerFactory.getLogger("com.holon.system.output")
    Logger.setLevel("NexmarkProducer", "INFO")

    def main(args: Array[String]): Unit = {
        setupConfig()

        val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        val host = kafkaBootstrapServers.split(":").head
        val port = kafkaBootstrapServers.split(":").last.toInt
        val PRODUCER_SLEEP_TIME_MS = sys.env.getOrElse("PRODUCER_SLEEP_TIME_MS", "100").toInt

        StartGate.waitUntilStarted(logger, "Waiting for start flag to be set.")
        Thread.sleep(2_000) // Let holon nodes start first

        if USE_LOG_FILE then
            outputLog.info(s"Starting holon.examples.nexmark.data.Nexmark producer with Kafka host: $host, port: $port, sleep time: $PRODUCER_SLEEP_TIME_MS")
        else
            logger.info(s"Starting holon.examples.nexmark.data.Nexmark producer with Kafka host: $host, port: $port, sleep time: $PRODUCER_SLEEP_TIME_MS")

        runProducer(host, port, PRODUCER_SLEEP_TIME_MS)
    }

    def runProducer(kafkaHost: String, kafkaPort: Int, sleepTime: Int): Unit = {
        logger.info(s"Starting holon.examples.nexmark.data.Nexmark producer with Kafka host: $kafkaHost, port: $kafkaPort, sleep time: $sleepTime")
        logger.info(s"outputting logs to ${if (USE_LOG_FILE) "log file" else s"console since USE_LOG_FILE is ${USE_LOG_FILE}"}")

        val producer = holon.streaming.messaging.KafkaLogProducer(kafkaHost, kafkaPort, KAFKA_TOPIC_INPUT)
        val iter = holon.examples.nexmark.data.Nexmark.iterator()

        val startTimeMs = System.currentTimeMillis()
        var producedInWindow = 0L
        var windowStartMs = startTimeMs

        // TODO: Only used for benchmarking.
        // Track the amount of input events per window (even if they are not the specific holon.crdt.CRDT class).
        val inputEventsPerWindow = mutable.Map.empty[Long, Long]

        while true do
            for i <- 0 until nrOfKafkaPartitions() do
                val batch = (0L until PRODUCER_BATCH_SIZE.toLong)
                    .map(_ => iter.next())
                    .map(x => {
                        val window = defineWindow(x.timestamp)
                        inputEventsPerWindow(window) = inputEventsPerWindow.getOrElse(window, 0L) + 1
                        val key = writeBinary(i)
                        val value = write(x) // Serialize the event as JSON
                        (key, value.getBytes("UTF-8"))
                    })
//                if USE_LOG_FILE then
//                    outputLog.info(s"[NexmarkProducer] Sending batch of size: ${batch.size} to partition: $i")
//                else
//                    logger.info(s"[NexmarkProducer] Sending batch of size: ${batch.size} to partition: $i")

                producer.send(batch)
                producedInWindow += PRODUCER_BATCH_SIZE.toLong

            producer.flush()

            // 3) Log per‐second production rate (including producerIndex):
            val nowMs = System.currentTimeMillis()
            if (nowMs - windowStartMs >= 1000) {
                val elapsedWindowMs = nowMs - windowStartMs
                val countThisSec = producedInWindow
                val secondKey = windowStartMs / 1000

                val msg =
                    s"[ProducerRate] producer 0 produced $countThisSec events " +
                      s"in last ${elapsedWindowMs}ms (partitions=${nrOfKafkaPartitions()}), second: $secondKey"

                if (USE_LOG_FILE) outputLog.info(msg) else logger.info(msg)

                producedInWindow = 0
                windowStartMs += 1000
            }

            // Get max key from inputEventsPerWindow
            val currentMaxWindow = inputEventsPerWindow.keys.max
            for ((k, v) <- inputEventsPerWindow) {
                if (k < currentMaxWindow) {
                    if USE_LOG_FILE then
                        outputLog.info(s"[Throughput] window: $k, eventCount: ${inputEventsPerWindow.getOrElse(k, 0)}")
                    else
                        logger.info(s"[Throughput] window: $k, eventCount: ${inputEventsPerWindow.getOrElse(k, 0)}")
                    inputEventsPerWindow -= k
                }
            }

            Thread.sleep(sleepTime)
    }

    def setupConfig(): Unit = {
        val N_NODES = sys.env.getOrElse("N_NODES", "2").toInt
        holon.core.Config.N_NODES = N_NODES
        val PARTITIONS_PER_NODE = sys.env.getOrElse("PARTITIONS_PER_NODE", "1").toInt
        holon.core.Config.PARTITIONS_PER_NODE = PARTITIONS_PER_NODE
        val WINDOW_L = sys.env.getOrElse("WINDOW_LENGTH", "10000").toLong
        holon.core.Config.WINDOW_LENGTH = WINDOW_L
    }

    def defineWindow(eventTime: Long): Long = {
        val time = eventTime / 10
        if (time % WINDOW_LENGTH == 0) time / WINDOW_LENGTH else (time / WINDOW_LENGTH) + 1
    }

}

