package holon.examples.taxi

import holon.utils.*
import holon.core.Config.*
import holon.crdt.serialization.WindowStateSerialization.OutputState
import org.slf4j.LoggerFactory
import upickle.legacy.*

import scala.collection.mutable

object OutputConsumer {

    def main(args: Array[String]): Unit = {
        val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
        val host = kafkaBootstrapServers.split(":").head
        val port = kafkaBootstrapServers.split(":").last.toInt

        consumeOutput(host, port)
    }
    def consumeOutput(kafkaHost: String, kafkaPort: Int): Unit = {
        val logger = Logger.apply("Consumer")
        val systemOutputLog = LoggerFactory.getLogger("com.holon.system.output")
        Logger.setLevel("Consumer", "INFO")
        val outputLagPerWindow = mutable.Map.empty[Long, Long]
        val outputPerWindow = mutable.Map.empty[Long, Map[Int, String]]

        val output = holon.streaming.messaging.KafkaLogConsumer(kafkaHost, kafkaPort, KAFKA_TOPIC_OUTPUT, (0 until nrOfKafkaPartitions()).toList)
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
                        val outputValue = outputState.value
                        val lagAppendTime = r._3
                        // Take the minimum output log append time for the window
                        outputLagPerWindow(windowId) =
                            if outputLagPerWindow.getOrElse(windowId, Long.MaxValue) == 0L then lagAppendTime
                            else math.min(outputLagPerWindow.getOrElse(windowId, Long.MaxValue), lagAppendTime)

                        outputPerWindow(windowId) = outputPerWindow.getOrElse(windowId, Map.empty[Int, String]) + (partition -> outputValue)

//                        if USE_LOG_FILE then
//                            systemOutputLog.info(s"[OUTPUT]: partition: $partition window: $windowId, value: $outputValue, lagAppendTime: $lagAppendTime")
//                        else
//                            logger.info(s"[OUTPUT]: partition: $partition window: $windowId, value: $outputValue, lagAppendTime: $lagAppendTime")

                        if (outputPerWindow(windowId).size == nrOfKafkaPartitions()) {
                            // if all strings for each partition are the same
                            val allSame = outputPerWindow(windowId).values.toSeq.distinct.size == 1
                            if allSame then
                                if USE_LOG_FILE then
                                    systemOutputLog.info(s"[CORRECT-OUTPUT]: partition: $partition window: $windowId, final value: $outputValue")
                                else
                                    logger.info(s"[CORRECT-OUTPUT]: partition: $partition window: $windowId, final value: $outputValue")
                                outputPerWindow.remove(windowId)
                            else
                                if USE_LOG_FILE then
                                    systemOutputLog.info(s"[INCORRECT-OUTPUT]: partition: $partition window: $windowId, final value: $outputValue")
                                else
                                    logger.info(s"[INCORRECT-OUTPUT]: partition: $partition window: $windowId, final value: $outputValue")
                                systemOutputLog.info(s"[INCORRECT-OUTPUT]: partition: $partition window: $windowId, final value: $outputValue")
                        }
                        val windowsToOutput = outputLagPerWindow.keys.filter(_ < windowId - 1).toList.sorted
                        windowsToOutput.foreach { winId =>
                             if USE_LOG_FILE then
                                 systemOutputLog.info(s"[LagAppendOutput] - window: $winId, timestamp: ${outputLagPerWindow(winId)}")
                             else
                                  logger.info(s"[LagAppendOutput] - window: $winId, timestamp: ${outputLagPerWindow(winId)}")
                             outputLagPerWindow.remove(winId)
                        }
    }
}