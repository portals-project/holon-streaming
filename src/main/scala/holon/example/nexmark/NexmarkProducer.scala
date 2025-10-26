package holon.example.nexmark

import holon.*
import holon.backend.*
import holon.example.Nexmark
import Config.*
import holon.backend.kafka.KafkaLogProducer
import upickle.default.*

import scala.collection.mutable

object NexmarkProducer {

    private val logger = Logger("NexmarkProducer")
    Logger.setLevel("NexmarkProducer", "INFO")

    def main(args: Array[String]): Unit = {
        setupConfig()

        val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
        val host = kafkaBootstrapServers.split(":").head
        val port = kafkaBootstrapServers.split(":").last.toInt
        val PRODUCER_SLEEP_TIME_MS = sys.env.getOrElse("PRODUCER_SLEEP_TIME_MS", "100").toInt

        runProducer(host, port, PRODUCER_SLEEP_TIME_MS)
    }

    def runProducer(kafkaHost: String, kafkaPort: Int, sleepTime: Int): Unit = {
        val producer = KafkaLogProducer(kafkaHost, kafkaPort, KAFKA_TOPIC_INPUT)
        val iter = Nexmark.iterator()

        while true do
            for i <- 0 until nrOfKafkaPartitions() do
                val batch = (0 until PRODUCER_BATCH_SIZE)
                    .map(_ => iter.next())
                    .map(x => {
                        (writeBinary(i), Nexmark.serialize(x))
                    })
                producer.send(batch)

            producer.flush()

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
}

