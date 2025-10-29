package holon.examples.nexmark.consumers

import holon.utils.*
import holon.core.Config.*
import org.slf4j.LoggerFactory
import upickle.default.*

import scala.util.Try

object OutputConsumerJson {

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
       ts: Long
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

    // define the implicit ReadWriter for InputRecord
    implicit val outputRecordRW: ReadWriter[OutputRecord] = macroRW
    implicit val outputRecordQ4RW: ReadWriter[OutputRecordQ4] = macroRW
    val outputLog = LoggerFactory.getLogger("com.holon.system.output")

    def main(args: Array[String]): Unit = {
        setupConfig()
        val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
        val host = kafkaBootstrapServers.split(":").head
        val port = kafkaBootstrapServers.split(":").last.toInt

//        val heartbeatThread = RunThread(this.consumeInputStream(host, port))
        consumeOutput(host, port)
    }



    def consumeOutput(kafkaHost: String, kafkaPort: Int): Unit = {
        val logger = Logger.apply("Consumer")
        Logger.setLevel("Consumer", "INFO")

        logger.info(s"Starting Output Consumer with Kafka host: $kafkaHost, port: $kafkaPort")
        logger.info(s"outputting logs to ${if (USE_LOG_FILE) "log file" else s"console since USE_LOG_FILE is ${USE_LOG_FILE}"}")

        val outputLagPerWindow = scala.collection.mutable.Map.empty[Long, Long]

        val output = holon.streaming.messaging.KafkaLogConsumer(kafkaHost, kafkaPort, KAFKA_TOPIC_OUTPUT, (0 until nrOfKafkaPartitions()).toList)
        while true do
            output.poll() match
                case Nil =>
                    Thread.sleep(CONSUMER_SLEEP_MS)
                case records =>
                    records.foreach: r =>
                        val jsonString = new String(r._2, "UTF-8")
                        if WORKLOAD != 4 then
                            Try(read[OutputRecord](jsonString)) match {
                                case scala.util.Success(inputRecord) =>
                                    val windowId = defineWindow(inputRecord.timestamp)
                                    val lagAppendTime = r._3

                                    outputLagPerWindow(windowId) =
                                        if outputLagPerWindow.getOrElse(windowId, Long.MaxValue) == 0L then lagAppendTime
                                        else math.min(outputLagPerWindow.getOrElse(windowId, Long.MaxValue), lagAppendTime)

                                    // if USE_LOG_FILE then
                                    // outputLog.info(s"[OUTPUT]: partition: $partition, window: $windowId, record: ${inputRecord.price}")
                                    // else logger.info(s"[OUTPUT]: partition: $partition, window: $windowId, record: ${inputRecord.price}")

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
                        else
                            Try(read[OutputRecordQ4](jsonString)) match {
                                case scala.util.Success(inputRecord) =>
                                    val windowId      = defineWindow(inputRecord.ts)
                                    val lagAppendTime = r._3

//                                     optional: print every Q4 record if you still want to see them
                                     if USE_LOG_FILE then
                                       outputLog.info(
                                         s"[OUTPUT] category=${inputRecord.category}, avg=${inputRecord.avg_win_price}, ts=${inputRecord.ts}, windowID: ${windowId} outputLagPerWindow: ${outputLagPerWindow.getOrElse(windowId, "N/A")}"
                                       )
                                     else
                                       logger.info(
                                         s"[OUTPUT] category=${inputRecord.category}, avg=${inputRecord.avg_win_price}, ts=${inputRecord.ts}"
                                       )

//                                     update to the LATEST append time for this window
                                    outputLagPerWindow(windowId) =
                                        outputLagPerWindow.get(windowId) match {
                                            case None                    => lagAppendTime
                                            case Some(previousAppendTime) =>
                                                math.max(previousAppendTime, lagAppendTime)
                                        }

                                    outputLagPerWindow(windowId) =
                                        if outputLagPerWindow.getOrElse(windowId, Long.MaxValue) == 0L then lagAppendTime
                                        else math.min(outputLagPerWindow.getOrElse(windowId, Long.MaxValue), lagAppendTime)


                                    // now see if any "old" windows have finished (current windowId - 1)
                                    val windowsToOutput = outputLagPerWindow.keys.filter(_ < windowId - 1).toList.sorted
    //                                outputLog.info(s"Windows to output: ${outputLagPerWindow.keys.mkString(", ")}")
                                    windowsToOutput.foreach { winId =>
                                        val finalAppendTs = outputLagPerWindow(winId)
                                        // log one line per completed window:
                                        if USE_LOG_FILE then
                                            outputLog.info(s"[LagAppendOutput] - window: $winId, timestamp: $finalAppendTs")
                                        else
                                            logger.info(s"[LagAppendOutput] - window: $winId, timestamp: $finalAppendTs")

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