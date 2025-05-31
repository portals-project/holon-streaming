package holon.example.nexmark

import holon.*
import holon.backend.*
import holon.example.Nexmark
import Config.*
import org.slf4j.LoggerFactory
import upickle.default.*

import scala.collection.mutable

object NexmarkProducer {
    private val FirestoreClient = holon.backend.cloud.FirestoreClient

    private val logger    = Logger("NexmarkProducer")
    private val outputLog = LoggerFactory.getLogger("com.holon.system.output")
    Logger.setLevel("NexmarkProducer", "INFO")

    def main(args: Array[String]): Unit = {
        setupConfig()

        val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
        val host = kafkaBootstrapServers.split(":").head
        val port = kafkaBootstrapServers.split(":").last.toInt
        val PRODUCER_SLEEP_TIME_MS = sys.env.getOrElse("PRODUCER_SLEEP_TIME_MS", "100").toInt

        logger.info(s"Set up the nexmark producer with these parameters: $host, $port, sleep=${PRODUCER_SLEEP_TIME_MS}ms")

        while (!FirestoreClient.isStartFlagSet) {
            logger.info("Waiting for start flag to be set.")
            Thread.sleep(1_000)
        }

        runProducer(host, port, PRODUCER_SLEEP_TIME_MS)
    }

    def runProducer(kafkaHost: String, kafkaPort: Int, sleepTime: Int): Unit = {
        val producer = KafkaLogProducer(kafkaHost, kafkaPort, KAFKA_TOPIC_INPUT)
        val iter     = Nexmark.iterator()

        // Track the amount of input events per window (for throughput logs)
        val inputEventsPerWindow = mutable.Map.empty[Long, Long]

        // Track per-second production counts (for later plotting)
        val perSecondProduced = mutable.Map.empty[Long, Long]

        val startTimeMs      = System.currentTimeMillis()
        var producedInWindow = 0L
        var windowStartMs    = startTimeMs

        logger.debug("Starting Nexmark Producer with number of partitions: " + nrOfKafkaPartitions())

        while true do
            // determine how big a batch we emit this iteration
            val elapsedSec = (System.currentTimeMillis() - startTimeMs) / 1000
//            val batchSize  = if (elapsedSec < 30) PRODUCER_BATCH_SIZE else PRODUCER_BATCH_SIZE * 200
            val batchSize = PRODUCER_BATCH_SIZE

            for i <- 0 until nrOfKafkaPartitions() do
                logger.debug(s"creating a batch for partition: $i (batchSize=$batchSize)")
                val batch = (0 until batchSize)
                  .map(_ => iter.next())
                  .map { x =>
                      val window = defineWindow(x.timestamp)
                      inputEventsPerWindow(window) = inputEventsPerWindow.getOrElse(window, 0L) + 1
                      (writeBinary(i), Nexmark.serialize(x))
                  }

                logger.debug(s"sending batch for partition: $i, batch size: ${batch.size}")
                producer.send(batch)
                producedInWindow += batch.size

            producer.flush()

            // per-second producer rate log & recording
            val nowMs = System.currentTimeMillis()
            if (nowMs - windowStartMs >= 1000) {
                // log how many we produced in the past second
                if USE_LOG_FILE then
                    outputLog.info(s"[ProducerRate] Produced $producedInWindow events in last ${nowMs - windowStartMs} ms")
                else  
                    logger.info(s"[ProducerRate] Produced $producedInWindow events in last ${nowMs - windowStartMs} ms")
                // store it keyed by the second since epoch
                val secondKey = windowStartMs / 1000
                perSecondProduced(secondKey) = producedInWindow

                producedInWindow = 0
                windowStartMs   += 1000
            }

            // existing throughput logging into outputLog
            val currentMaxWindow = if (inputEventsPerWindow.nonEmpty) inputEventsPerWindow.keys.max else -1L
            for ((k, v) <- inputEventsPerWindow if k < currentMaxWindow) {
              if USE_LOG_FILE then
                outputLog.info(s"[Throughput] window: $k, eventCount: $v")
              else  
                logger.info(s"[Throughput] window: $k, eventCount: $v")
              inputEventsPerWindow -= k
            }

            Thread.sleep(sleepTime)
    }

    def setupConfig(): Unit = {
        val N_NODES = sys.env.getOrElse("N_NODES", "3").toInt
        Config.N_NODES = N_NODES
        val PARTITIONS_PER_NODE = sys.env.getOrElse("PARTITIONS_PER_NODE", "2").toInt
        Config.PARTITIONS_PER_NODE = PARTITIONS_PER_NODE
        logger.info(s"Producer set up config with N_NODES: $N_NODES and $PARTITIONS_PER_NODE partitions per node")
    }

    def defineWindow(eventTime: Long): Long = {
        val time = eventTime / 10
        if (time % WINDOW_LENGTH == 0) time / WINDOW_LENGTH else (time / WINDOW_LENGTH) + 1
    }
}
