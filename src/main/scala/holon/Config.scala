package holon

object Config {
  final val CONSUMER_SLEEP_MS = 100
  var SLEEP_BETWEEN_POLLS = 0L
  
  final val WORK_STEALING_THRESHOLD = 10_0000000   // was 10!! Determines how many (empty) polls a node should perform before it starts work stealing.
  var WORK_STEAL_ATTEMPT_COOLDOWN = 2000 // How long to wait before trying to steal work again.
  var HEARTBEAT_INTERVAL = 500L
  var FAILURE_DETECTION_THRESHOLD = 2_000000L // was 2000L
  var CHECKPOINT_INTERVAL = 10_000L

  var RUN_FLINK_PRODUCER: Boolean = sys.env.get("RUN_FLINK_PRODUCER").map(_.toBoolean).getOrElse(true)

  // HOLON cluster size
  var N_NODES: Int = sys.env.get("N_NODES").map(_.toInt).getOrElse(10)

  var PARTITIONS_PER_NODE: Int = sys.env.get("PARTITIONS_PER_NODE").map(_.toInt).getOrElse(5)
  
  var WORKLOAD: Int = sys.env.get("WORKLOAD").map(_.toInt).getOrElse(0) // 0 = Q0, 4 = Q4, 7 = Q7

  // PRODUCER configuration
  var PRODUCER_SLEEP_MS: Int = sys.env.get("PRODUCER_SLEEP_MS").map(_.toInt).getOrElse(100)

  var PRODUCER_BATCH_SIZE: Long = sys.env.get("PRODUCER_BATCH_SIZE").map(_.toLong).getOrElse(1024L)

  var PRODUCER_COUNT: Int = sys.env.get("PRODUCER_COUNT").map(_.toInt).getOrElse(50)

  var EVENTS_PER_SECOND: Long = sys.env.get("EVENTS_PER_SECOND").map(_.toLong).getOrElse(10_000L)

  var WINDOW_LENGTH: Long = sys.env.get("WINDOW_LENGTH").map(_.toLong).getOrElse(10_000L)

  var GARBAGE_COLLECTION_OFFSET: Long = sys.env.get("GARBAGE_COLLECTION_OFFSET").map(_.toLong).getOrElse(10L)

  var FLUSH_THRESHOLD: Int = sys.env.get("FLUSH_THRESHOLD").map(_.toInt).getOrElse(500)

  var ENABLE_RATE_LIMIT: Boolean = sys.env.get("ENABLE_RATE_LIMIT").map(_.toBoolean).getOrElse(false)

  var PROCESSING_RATE_LIMIT: Int = sys.env.get("PROCESSING_RATE_LIMIT").map(_.toInt).getOrElse(10_000)

  var TOPIC_METRICS_INTERVAL: Long = sys.env.get("TOPIC_METRICS_INTERVAL").map(_.toLong).getOrElse(5_000L)

  var USE_LOG_FILE: Boolean = sys.env.get("USE_LOG_FILE").map(_.toBoolean).getOrElse(true)
  
  var RUN_MAX_THROUGHPUT_PRODUCER: Boolean = sys.env.get("RUN_MAX_THROUGHPUT_PRODUCER").map(_.toBoolean).getOrElse(false)

// KAFKA AND CHANNELS   
  final val KAFKA_TOPIC_INPUT = "input"
  final val KAFKA_TOPIC_BROADCAST = "broadcast"
  final val KAFKA_TOPIC_CONTROL = "control"
  final val KAFKA_TOPIC_OUTPUT = "output"
  final val CHN_INPUT = 0x00
  final val CHN_BROADCAST = 0x01
  final val CHN_CONTROL = 0x02
  final val CHN_OUTPUT = 0x03
  final val BROADCAST_PARTITION_ID = -1
  final val CONTROL_PARTITION_ID = -2

  var KAFKA_HOST = "localhost"
  var KAFKA_PORT = 9092

  // GOOGLE CLOUD STORAGE
  final val USE_CLOUD_STORAGE_CHECKPOINTS = false
  final val GCS_BUCKET_NAME = "failure-recovery-dev"
  // Update locally
  final val GC_CREDENTIALS_FILE_PATH = "C:/Users/rvang/Documents/GitHub/holon-streaming-clone/.gcp/gcs-service-account.json"
  // Firestore key
  val FIRESTORE_START_KEY = "flags_ruben"

  def nrOfKafkaPartitions(): Int = {
    N_NODES * PARTITIONS_PER_NODE
  }
}
