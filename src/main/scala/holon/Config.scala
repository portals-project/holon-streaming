package holon

object Config {
  var N_NODES = 2
  var PARTITIONS_PER_NODE = 2
  var KAFKA_N_PARTITIONS = N_NODES * PARTITIONS_PER_NODE
  final val KAFKA_TOPIC_INPUT = "input"
  final val KAFKA_TOPIC_BROADCAST = "broadcast"
  final val KAFKA_TOPIC_CONTROL = "control"
  final val KAFKA_TOPIC_OUTPUT = "output"
  final val CHECKPOINT_INTERVAL = 10_000L
  final val PRODUCER_BATCH_SIZE = 1024
  final val PRODUCER_SLEEP_MS = 100
  final val CONSUMER_SLEEP_MS = 100

  final val CHN_INPUT = 0x00
  final val CHN_BROADCAST = 0x01
  final val CHN_CONTROL = 0x02
  final val CHN_OUTPUT = 0x03

  final val BROADCAST_PARTITION_ID = -1
  final val CONTROL_PARTITION_ID = -2

  final val USE_CLOUD_STORAGE_CHECKPOINTS = false
  final val GCS_BUCKET_NAME = "failure-recovery-dev" // "windowed-aggregations-dev"
  final val GC_CREDENTIALS_FILE_PATH = "/Users/kolya/kth_projects/holon-streaming/.gcp/gcs-service-account.json"

  final val WORK_STEALING_THRESHOLD = 10   // Determines how many (empty) polls a node should perform before it starts work stealing.
  var SLEEP_BETWEEN_POLLS = 0L

  var KAFKA_HOST = "localhost"
  var KAFKA_PORT = 9092
}
