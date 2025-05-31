package holon

object Config {
  final val KAFKA_TOPIC_INPUT = "input"
  final val KAFKA_TOPIC_BROADCAST = "broadcast"
  final val KAFKA_TOPIC_CONTROL = "control"
  final val KAFKA_TOPIC_OUTPUT = "output"
  final val PRODUCER_SLEEP_MS = 100
  final val CONSUMER_SLEEP_MS = 100
  var CHECKPOINT_INTERVAL = 10_000L
  var PRODUCER_BATCH_SIZE: Int = 1024

  final val CHN_INPUT = 0x00
  final val CHN_BROADCAST = 0x01
  final val CHN_CONTROL = 0x02
  final val CHN_OUTPUT = 0x03

  final val BROADCAST_PARTITION_ID = -1
  final val CONTROL_PARTITION_ID = -2

  final val USE_CLOUD_STORAGE_CHECKPOINTS = false
  final val GCS_BUCKET_NAME = "failure-recovery-dev"
  // Update locally
  final val GC_CREDENTIALS_FILE_PATH = "/Users/rvang/Documents/GitHub/holon-streaming-clone/.gcp/gcs-service-account.json"

  // TODO: was 10, change value to enable work stealing again
  final val WORK_STEALING_THRESHOLD = 10_0000000   // Determines how many (empty) polls a node should perform before it starts work stealing.
  var WORK_STEAL_ATTEMPT_COOLDOWN = 2000 // How long to wait before trying to steal work again.
  var SLEEP_BETWEEN_POLLS = 0L

  var N_NODES = 2
  var PARTITIONS_PER_NODE = 2

  var HEARTBEAT_INTERVAL = 500L
  var FAILURE_DETECTION_THRESHOLD = 2_000000L

  var KAFKA_HOST = "localhost"
  var KAFKA_PORT = 9092

  var WINDOW_LENGTH = 5_000L
  // TODO: Test if this works, might need to be dynamic
  // Buffer size for garbage collection roughly equals the # of windows that will be kept in checkpointed
  val GARBAGE_COLLECTION_OFFSET = 10L
  val FLUSH_THRESHOLD = 500 // How many messages to buffer before flushing to Kafka
  val ENABLE_RATE_LIMIT = false // Enable rate limiting for the recovery module
  val PROCESSING_RATE_LIMIT = 10_000 // Records should be processed by each node
  val TOPIC_METRICS_INTERVAL = 5_000L // How often to log topic metrics

  val USE_LOG_FILE = true // Will log output to a file instead of the console

  // Update locally
  val FIRESTORE_START_KEY = "flags_ruben"

  def nrOfKafkaPartitions(): Int = {
    N_NODES * PARTITIONS_PER_NODE
  }
}
