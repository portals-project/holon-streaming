package holon.examples.taxi

import holon.utils.*
import holon.core.Config.*
import holon.core.Utils.*
import holon.core.{ConsumerRef, ProducerRef}

object HolonNode {

    private val FirestoreClient = holon.streaming.cloud.FirestoreClient

    private val logger = Logger("HolonNode")
    Logger.setLevel("HolonNode", "INFO")

    def main(args: Array[String]): Unit = {
        setupConfig()
        val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
        val RUNTIME = sys.env.getOrElse("RUNTIME", "60000").toInt
        val nodeId: Int =
            args.headOption.map(_.toInt)
              .orElse(sys.env.get("NODE_ID").map(_.toInt))
              .getOrElse {
                  throw new IllegalArgumentException("Must supply node-id as first arg or via NODE_ID")
              }

        while (!FirestoreClient.isStartFlagSet) {
            logger.info("Node: ${NodeId} Waiting for start flag to be set.")
            Thread.sleep(500)
        }

        SafeRun(RUNTIME) {
            val partitions = (nodeId * PARTITIONS_PER_NODE until (nodeId + 1) * PARTITIONS_PER_NODE).toList
            System.out.println(s" Node: $nodeId Partitions: $partitions")
            val kafkaHost = kafkaBootstrapServers.split(":").head
            val kafkaPort = kafkaBootstrapServers.split(":").last.toInt
            val j = job(partitions, kafkaHost, kafkaPort)
            val holonInstance = holon.core.Holon(nodeId)
            holonInstance.submitOrUpdate(j)
            Thread.sleep(RUNTIME)
        }
    }

    def setupConfig(): Unit = {
        holon.core.Config.KAFKA_HOST = "kafka"
        holon.core.Config.KAFKA_PORT = 9093

        val N_NODES = sys.env.getOrElse("N_NODES", "2").toInt
        holon.core.Config.N_NODES = N_NODES
        val PARTITIONS_PER_NODE = sys.env.getOrElse("PARTITIONS_PER_NODE", "2").toInt
        holon.core.Config.PARTITIONS_PER_NODE = PARTITIONS_PER_NODE

        val sleepBetweenPolls = sys.env.getOrElse("SLEEP_BETWEEN_POLLS", "0").toLong
        holon.core.Config.SLEEP_BETWEEN_POLLS = sleepBetweenPolls
    }

    // holon.core.Job function creates a job object with the specified consumers and producers
    def job(partitions: List[Int], kafkaHost: String, kafkaPort: Int): holon.core.Job = {

        val nexmarkConsumers = partitions.map { partition =>
            consumerRef(CHN_INPUT, KAFKA_TOPIC_INPUT, partition, kafkaHost, kafkaPort)
        }
        val internalConsumers = List(
            consumerRef(CHN_BROADCAST, KAFKA_TOPIC_BROADCAST, 0, kafkaHost, kafkaPort),
            consumerRef(CHN_CONTROL, KAFKA_TOPIC_CONTROL, 0, kafkaHost, kafkaPort)
            )
        val consumers = nexmarkConsumers ++ internalConsumers

        val producers = List(
            producerRef(CHN_INPUT, KAFKA_TOPIC_INPUT, kafkaHost, kafkaPort),
            producerRef(CHN_BROADCAST, KAFKA_TOPIC_BROADCAST, kafkaHost, kafkaPort),
            producerRef(CHN_CONTROL, KAFKA_TOPIC_CONTROL, kafkaHost, kafkaPort),
            producerRef(CHN_OUTPUT, KAFKA_TOPIC_OUTPUT, kafkaHost, kafkaPort),
            )

        val procFunFactory: holon.streaming.processing.ProcFunFactory = new TQ2Factory()

        val job = holon.core.Job(
            consumers = consumers,
            producers = producers,
            procFunFactory = procFunFactory,
            partitions = partitions,
            )
        job
    }

    def consumerRef(chn: Byte, topic: String, partition: Int, host: String, port: Int): ConsumerRef =
        ConsumerRef(
            chn = chn,
            host = host,
            port = port,
            topic = topic,
            partitions = List(partition),
            )

    def producerRef(chn: Byte, topic: String, host: String, port: Int): ProducerRef =
        ProducerRef(
            chn = chn,
            host = host,
            port = port,
            topic = topic,
            )
}
