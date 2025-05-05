package holon.example.nexmark

import holon.*
import holon.Config.*
import holon.backend.*
import upickle.legacy.*

object OutputConsumer {

    def main(args: Array[String]): Unit = {
        val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
        val host = kafkaBootstrapServers.split(":").head
        val port = kafkaBootstrapServers.split(":").last.toInt

        consumeOutput(host, port)
    }

    def consumeOutput(kafkaHost: String, kafkaPort: Int): Unit = {
        val logger = Logger.apply("Consumer")
        Logger.setLevel("Consumer", "INFO")

        val outputLagPerWindow = scala.collection.mutable.Map.empty[Long, Long]

        val output = KafkaLogConsumer(kafkaHost, kafkaPort, KAFKA_TOPIC_OUTPUT, (0 until nrOfKafkaPartitions()).toList)
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
                        val logAppendTime = r._3
                        // Take the minimum output log append time for the window
                        outputLagPerWindow(windowId) =
                            if outputLagPerWindow.getOrElse(windowId, Long.MaxValue) == 0L then logAppendTime
                            else math.min(outputLagPerWindow.getOrElse(windowId, Long.MaxValue), logAppendTime)

                        logger.info(s"[OUTPUT]: partition: $partition window: $windowId, value: $outputValue")

                        val windowsToOutput = outputLagPerWindow.keys.filter(_ < windowId - 1).toList.sorted
                        windowsToOutput.foreach { winId =>
                            logger.info(s"[LagAppendOutput] - window: $winId, timestamp: ${outputLagPerWindow(winId)}")
                            outputLagPerWindow.remove(winId)
                        }
    }
}