package holon.example.nexmark

object Config {
  final val KAFKA_HOST = "localhost"
  final val KAFKA_PORT = 9092
  final val KAFKA_TOPIC_NEXMARK = "nexmark"
  final val KAFKA_TOPIC_BROADCAST = "broadcast"
  final val KAFKA_TOPIC_OUTPUT = "output"
  final val N_NODES = 2
  final val PARTITIONS_PER_NODE = 2
  final val KAFKA_N_PARTITIONS = N_NODES * PARTITIONS_PER_NODE
  final val PRODUCER_BATCH_SIZE = 1024
  final val PRODUCER_SLEEP_MS = 100
  final val CONSUMER_SLEEP_MS = 100

  final val CHN_NEXMARK = 0x00
  final val CHN_BROADCAST = 0x01
  final val CHN_OUTPUT = 0x02

}
