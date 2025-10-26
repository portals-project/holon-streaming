package holon.examples.nexmark

import holon.utils.*
import holon.core.Config.*
import org.slf4j.LoggerFactory
import upickle.default.*

import scala.util.Try

object LagAppendOutputConsumer {

    case class OutputRecord(
        auction: Long,
        price: Long,
        bidder: Long,
        dateTime: String,
        timestamp: Long,
        extra: String
    )

    case class OutputRecordQ4(
         category: String,
         avg_win_price: Double,
         timestamp: Long
   )

    case class InputRecord(
        eventType: String,
        timestamp: Long
    )

    object InputRecord {
        implicit val rw: ReadWriter[InputRecord] = readwriter[ujson.Value].bimap[InputRecord](
            record => ujson.Obj(
                "event" -> ujson.Obj("$type" -> record.eventType),
                "timestamp" -> record.timestamp
                ),
            json => {
                val eventType = json("event")("$type").str
                val timestamp = json("timestamp").num.toLong
                InputRecord(eventType, timestamp)
            }
            )
    }

    // Define the implicit ReadWriter for InputRecord
    implicit val outputRecordRW: ReadWriter[OutputRecord] = macroRW
    val outputLog = LoggerFactory.getLogger("com.holon.system.output")

    def main(args: Array[String]): Unit = {
        setupConfig()
        val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
        val host = kafkaBootstrapServers.split(":").head
        val port = kafkaBootstrapServers.split(":").last.toInt

        consumeInputStreamThreaded(host, port)
//        consumeOutput(host, port)
    }

    import scala.concurrent.{ExecutionContext, Future}
    import java.util.concurrent.Executors

    private def consumeInputStreamThreaded(kafkaHost: String, kafkaPort: Int): Unit = {
        val logger = Logger.apply("InputStream")
        Logger.setLevel("InputStream", "INFO")

        val partitions = (0 until nrOfKafkaPartitions()).toList
        val executor = Executors.newFixedThreadPool(partitions.size)
        implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

        logger.info(s"Starting InputStream Consumer with Kafka host: $kafkaHost, port: $kafkaPort")
        logger.info(s"outputting logs to ${if (USE_LOG_FILE) "log file" else s"console since USE_LOG_FILE is ${USE_LOG_FILE}"}")

        partitions.foreach { partition =>
            Future {
                val input = holon.streaming.messaging.KafkaLogConsumer(kafkaHost, kafkaPort, KAFKA_TOPIC_INPUT, List(partition))
                val lastAppendPerWindow = scala.collection.mutable.Map.empty[Long, Long]

                while (true) {
                    input.poll() match
                        case Nil =>
                            Thread.sleep(CONSUMER_SLEEP_MS)
                        case records =>
                            records.foreach: r =>
                                val jsonString = new String(r._2, "UTF-8")
                                Try(read[InputRecord](jsonString)) match {
                                    case scala.util.Success(inputRecord) =>
                                        if (inputRecord.eventType.contains("Bid") || inputRecord.eventType.contains("Auction")) {
                                            val windowKey = defineWindow(inputRecord.timestamp)

                                            lastAppendPerWindow(windowKey) =
                                                math.max(lastAppendPerWindow.getOrElse(windowKey, 0L), r._3)

                                            val windowsToOutput = lastAppendPerWindow.keys.filter(_ < windowKey - 1).toList.sorted
                                            windowsToOutput.foreach { winId =>
                                              if USE_LOG_FILE then
                                                outputLog.info(s"[LagAppendInput] - partition: $partition, window: $windowKey, timestamp: ${lastAppendPerWindow(winId)}")
                                              else logger.info(s"[LagAppendInput] - partition: $partition, window: $windowKey, timestamp: ${lastAppendPerWindow(winId)}")
                                                lastAppendPerWindow.remove(winId)
                                            }
                                        }
                                    case scala.util.Failure(exception) =>
                                        logger.error(s"Failed to parse JSON: $jsonString, error: ${exception.getMessage}")
                                }
                }
            }
        }
    }



    def consumeOutput(kafkaHost: String, kafkaPort: Int): Unit = {
        val logger = Logger.apply("Consumer")
        Logger.setLevel("Consumer", "INFO")

        val outputLagPerWindow = scala.collection.mutable.Map.empty[Long, Long]

        val output = holon.streaming.messaging.KafkaLogConsumer(kafkaHost, kafkaPort, KAFKA_TOPIC_OUTPUT, (0 until nrOfKafkaPartitions()).toList)
        while true do
            output.poll() match
                case Nil =>
                    Thread.sleep(CONSUMER_SLEEP_MS)
                case records =>
                    records.foreach: r =>
                        val jsonString = new String(r._2, "UTF-8")
                        Try(read[OutputRecord](jsonString)) match {
                            case scala.util.Success(inputRecord) =>
                                val windowId = defineWindow(inputRecord.timestamp)
                                val lagAppendTime = r._3

                                outputLagPerWindow(windowId) =
                                    if outputLagPerWindow.getOrElse(windowId, Long.MaxValue) == 0L then lagAppendTime
                                    else math.min(outputLagPerWindow.getOrElse(windowId, Long.MaxValue), lagAppendTime)

//                                if USE_LOG_FILE then
//                                    outputLog.info(s"[OUTPUT]: partition: $partition, window: $windowId, record: ${inputRecord.price}")
//                                else logger.info(s"[OUTPUT]: partition: $partition, window: $windowId, record: ${inputRecord.price}")

                                val windowsToOutput = outputLagPerWindow.keys.filter(_ < windowId - 1).toList.sorted
                                windowsToOutput.foreach { winId =>
                                    if USE_LOG_FILE then
                                        outputLog.info(s"[LagAppendOutput] - window: $winId, timestamp: ${outputLagPerWindow(winId)}")
                                    else logger.info(s"[LagAppendOutput] - window: $winId, timestamp: ${outputLagPerWindow(winId)}")
                                    outputLagPerWindow.remove(winId)
                                }
                            case scala.util.Failure(exception) =>
                                logger.error(s"Failed to parse JSON: $jsonString, error: ${exception.getMessage}")
                        }
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