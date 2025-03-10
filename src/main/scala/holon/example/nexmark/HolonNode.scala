package holon.example.nexmark

import holon.Holon
import upickle.default.*

import holon.*
import holon.backend.*
import holon.example.nexmark.Config.*
import holon.Utils.*

object HolonNode {

    def main(args: Array[String]): Unit = {
        val RUNTIME = sys.env.getOrElse("RUNTIME", "60000").toInt
        val nodeId = sys.env.getOrElse("NODE_ID", "0").toInt
        val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")

        SafeRun(RUNTIME) {
            val partitions = (nodeId * PARTITIONS_PER_NODE until (nodeId + 1) * PARTITIONS_PER_NODE).toList
            System.out.println(s" Node: $nodeId Partitions: $partitions")
            val j = job(partitions, kafkaBootstrapServers)
            val holon = Holon(nodeId)
            holon.submitOrUpdate(j)
            Thread.sleep(RUNTIME)
        }
    }

    def job(partitions: List[Int], kafkaBootstrapServer: String): Job = {
        val kafka_host = kafkaBootstrapServer.split(":").head
        val kafka_port = kafkaBootstrapServer.split(":").last.toInt

        val consumers = partitions.map { partition =>
            consumerRef(CHN_NEXMARK, KAFKA_TOPIC_NEXMARK, partition, kafka_host, kafka_port)
        } :+ consumerRef(CHN_BROADCAST, KAFKA_TOPIC_BROADCAST, 0, kafka_host, kafka_port)

        val producers = List(
            producerRef(CHN_NEXMARK, KAFKA_TOPIC_NEXMARK, kafka_host, kafka_port),
            producerRef(CHN_BROADCAST, KAFKA_TOPIC_BROADCAST, kafka_host, kafka_port),
            producerRef(CHN_OUTPUT, KAFKA_TOPIC_OUTPUT, kafka_host, kafka_port),
            )

        val job = Job(
            consumers = consumers,
            producers = producers,
            partitions = partitions,
            )

        job
    }

    private def consumerRef(chn: Byte, topic: String, partition: Int, host: String, port: Int) =
        ConsumerRef(
            chn = chn,
            host = host,
            port = port,
            topic = topic,
            partitions = List(partition),
            )

    private def producerRef(chn: Byte, topic: String, host: String, port: Int) =
        ProducerRef(
            chn = chn,
            host = host,
            port = port,
            topic = topic,
            )
}
