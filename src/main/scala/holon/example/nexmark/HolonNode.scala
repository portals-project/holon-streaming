package holon.example.nexmark

import holon.*
import Config.*
import holon.Utils.*

object HolonNode {

    def main(args: Array[String]): Unit = {
        Config.KAFKA_HOST = "kafka"
        Config.KAFKA_PORT = 9093

        val N_NODES = sys.env.getOrElse("N_NODES", "2").toInt
        Config.N_NODES = N_NODES
        val PARTITIONS_PER_NODE = sys.env.getOrElse("PARTITIONS_PER_NODE", "2").toInt
        Config.PARTITIONS_PER_NODE = PARTITIONS_PER_NODE
        KAFKA_N_PARTITIONS = N_NODES * PARTITIONS_PER_NODE

        val RUNTIME = sys.env.getOrElse("RUNTIME", "60000").toInt
        val nodeId = sys.env.getOrElse("NODE_ID", "0").toInt
        val sleepBetweenPolls = sys.env.getOrElse("SLEEP_BETWEEN_POLLS", "0").toLong
        Config.SLEEP_BETWEEN_POLLS = sleepBetweenPolls

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

        val nexmarkConsumers = partitions.map { partition =>
            consumerRef(CHN_INPUT, KAFKA_TOPIC_INPUT, partition, kafka_host, kafka_port)
        }
        val internalConsumers = List(
            consumerRef(CHN_BROADCAST, KAFKA_TOPIC_BROADCAST, 0, kafka_host, kafka_port),
            consumerRef(CHN_CONTROL, KAFKA_TOPIC_CONTROL, 0, kafka_host, kafka_port)
            )
        val consumers = nexmarkConsumers ++ internalConsumers

        val producers = List(
            producerRef(CHN_INPUT, KAFKA_TOPIC_INPUT, kafka_host, kafka_port),
            producerRef(CHN_BROADCAST, KAFKA_TOPIC_BROADCAST, kafka_host, kafka_port),
            producerRef(CHN_CONTROL, KAFKA_TOPIC_CONTROL, kafka_host, kafka_port),
            producerRef(CHN_OUTPUT, KAFKA_TOPIC_OUTPUT, kafka_host, kafka_port),
            )

        val job = Job(
            consumers = consumers,
            producers = producers,
//            procFunFactory = new RecordProcFunFactory(),
//            procFunFactory = new WindowedRecordProcFunFactory(),
            procFunFactory = new AuctionWindowedRecordProcFunFactory(),
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
