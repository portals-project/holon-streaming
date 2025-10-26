package holon.example.nexmark

import holon.*
import holon.Config.*
import holon.backend.*
import holon.backend.kafka.KafkaLogConsumer
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

                        logger.info(s"[OUTPUT]: partition: $partition window: $windowId, value: $outputValue")
    }
}