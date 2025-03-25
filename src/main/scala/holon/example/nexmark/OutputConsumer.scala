package holon.example.nexmark

import holon.*
import holon.backend.*
import holon.example.nexmark.Config.*
import upickle.default.*

object OutputConsumer {

    def main(args: Array[String]): Unit = {
        val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
        val host = kafkaBootstrapServers.split(":").head
        val port = kafkaBootstrapServers.split(":").last.toInt

        val logger = Logger.apply("Consumer")
        Logger.setLevel("Consumer", "INFO")
        val output = KafkaLogConsumer(host, port, KAFKA_TOPIC_OUTPUT, (0 until KAFKA_N_PARTITIONS).toList)

        while true do
            output.poll() match
                case Nil =>
                    Thread.sleep(CONSUMER_SLEEP_MS)
                case records =>
                    records.foreach: r =>
                        val crdtValue = readBinary[BigInt](r._2)
                        logger.info(s"[OUTPUT]: Closed a window with final aggregate: $crdtValue")
    }

}
