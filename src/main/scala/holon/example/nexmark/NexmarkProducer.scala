package holon.example.nexmark

import holon.*
import holon.backend.*
import holon.example.Nexmark
import Config.*
import upickle.default.*

object NexmarkProducer {
    def main(args: Array[String]): Unit = {
        val PRODUCER_SLEEP_TIME_MS = sys.env.getOrElse("PRODUCER_SLEEP_TIME_MS", "100").toInt
        val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
        val host = kafkaBootstrapServers.split(":").head
        val port = kafkaBootstrapServers.split(":").last.toInt

        val N_NODES = sys.env.getOrElse("N_NODES", "2").toInt
        Config.N_NODES = N_NODES
        val PARTITIONS_PER_NODE = sys.env.getOrElse("PARTITIONS_PER_NODE", "2").toInt
        Config.PARTITIONS_PER_NODE = PARTITIONS_PER_NODE
        KAFKA_N_PARTITIONS = N_NODES * PARTITIONS_PER_NODE
        println(s"KAFKA_N_PARTITIONS: $KAFKA_N_PARTITIONS")

        val producer = KafkaLogProducer(host, port, KAFKA_TOPIC_INPUT)
        val iter = Nexmark.iterator()
        while true do
            for i <- 0 until KAFKA_N_PARTITIONS do
                val batch = (0 until PRODUCER_BATCH_SIZE)
                    .map(_ => iter.next())
                    .map(x => (writeBinary(i), Nexmark.serialize(x)))
                producer.send(batch)

            producer.flush()
            Thread.sleep(PRODUCER_SLEEP_TIME_MS)
    }
}

