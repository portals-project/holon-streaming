package holon.example.nexmark

import holon.*
import holon.Utils.*
import holon.backend.*
import holon.example.Nexmark
import holon.example.nexmark.Config.*
import upickle.default.*

object NexmarkProducer {
    def main(args: Array[String]): Unit = {
        val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
        val host = kafkaBootstrapServers.split(":").head
        val port = kafkaBootstrapServers.split(":").last.toInt

        val producer = KafkaLogProducer(host, port, KAFKA_TOPIC_NEXMARK)
        val iter = Nexmark.iterator()
        while true do
            for i <- 0 until KAFKA_N_PARTITIONS do
                val batch = (0 until PRODUCER_BATCH_SIZE)
                    .map(_ => iter.next())
                    .map(x => (writeBinary(i), Nexmark.serialize(x)))
                producer.send(batch)

            producer.flush()
            Thread.sleep(PRODUCER_SLEEP_MS)
    }
}

