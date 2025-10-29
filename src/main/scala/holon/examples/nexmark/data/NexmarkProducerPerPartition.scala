package holon.examples.nexmark.data

import holon.utils.*
import holon.core.Config.*
import org.slf4j.LoggerFactory
import upickle.default.*

import scala.collection.mutable

object NexmarkProducerPerPartition {
  private val FirestoreClient = holon.streaming.cloud.FirestoreClient
  private val logger = Logger("NexmarkProducerPerPartition")
  private val outputLog = LoggerFactory.getLogger("com.holon.system.output")
  Logger.setLevel("NexmarkProducerPerPartition", "INFO")

  def main(args: Array[String]): Unit = {
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", s"$KAFKA_HOST:$KAFKA_PORT")
    val host = kafkaBootstrapServers.split(":").head
    val port = kafkaBootstrapServers.split(":").last.toInt

    // read only PRODUCER_INDEX from environment; everything else comes from holon.core.Config.scala
    val producerIndex: Int =
      args.headOption.map(_.toInt)
        .orElse(sys.env.get("PRODUCER_INDEX").map(_.toInt))
        .getOrElse {
          throw new IllegalArgumentException("Must supply producer-index as first arg or via PRODUCER_INDEX")
        }
    val producerCount = holon.core.Config.PRODUCER_COUNT
    val eventsPerPartition = holon.core.Config.PRODUCER_BATCH_SIZE
    val sleepTimeMs = holon.core.Config.PRODUCER_SLEEP_MS

    logger.info(
      s"Starting NexmarkProducer: host=$host, port=$port, " +
        s"sleep=${sleepTimeMs}ms, " +
        s"eventsPerPartition=${eventsPerPartition}, " +
        s"producerCount=${producerCount}, " +
        s"producerIndex=${producerIndex}"
    )

    // wait until Firestore "start" flag is set
    while (!FirestoreClient.isStartFlagSet) {
      logger.info(s"producer: ${producerIndex} Waiting for start flag to be set.")
      Thread.sleep(1_000)
    }

    // compute total number of Kafka partitions (N_NODES * PARTITIONS_PER_NODE)
    val totalPartitions = nrOfKafkaPartitions()

    val (myStart, myEnd) = computePartitionRange(totalPartitions, producerCount, producerIndex)
    val myPartitions: Vector[Int] = (myStart until myEnd).toVector

    val maxEps: Long = computeMaximumEventsPerSecond(totalPartitions, producerCount, eventsPerPartition, sleepTimeMs)

    if USE_LOG_FILE then
      outputLog.info(
        s"Producer #$producerIndex will produce to partitions [$myStart .. ${myEnd - 1}], " +
          s"myPartitions: ${myPartitions.mkString(",")}. max total events per second: $maxEps events/sec"
      )
    else logger.info(s"Producer #$producerIndex will produce to partitions [$myStart .. ${myEnd - 1}], " + s"myPartitions: ${myPartitions.mkString(",")}. max total events per second: $maxEps events/sec")

    // pass producerIndex into runProducer:
    runProducer(host, port, sleepTimeMs, eventsPerPartition, myPartitions, producerIndex)
  }

/**
 * Compute the partition range for the current producer.
 * @param totalPartitions The total number of Kafka partitions.
 * @param producerCount The number of producers.
 * @param producerIndex The index of the current producer.
 * @return A tuple containing the start and end partition indices.
 */
  private def computePartitionRange(totalPartitions: Int, producerCount: Int, producerIndex:Int): (Int, Int) = {
    val perProd = totalPartitions / producerCount
    val extra   = totalPartitions % producerCount

    if (producerIndex < extra) {
      val start = producerIndex * (perProd + 1)
      (start, start + (perProd + 1))
    } else {
      val start = extra * (perProd + 1) + (producerIndex - extra) * perProd
      (start, start + perProd)
    }
  }

  private def computeMaximumEventsPerSecond(totalPartitions: Int, producerCount: Int, eventsPerPartition: Long, sleepTimeMs:     Int): Long = {
    val partitionsPerProducer = totalPartitions / producerCount
    val ticksPerSecond = 1000.0 / sleepTimeMs
    math.floor(partitionsPerProducer.toLong * eventsPerPartition.toLong * ticksPerSecond).toLong
  }

  /**
   * Main loop: for each partition in myPartitions, generate `eventsPerPartition` events,
   * send them to Kafka, then sleep for `sleepTimeMs`.
   * Also log per‐second and per‐window stats.
   */
  private def runProducer(
                           kafkaHost: String,
                           kafkaPort: Int,
                           sleepTimeMs: Int,
                           eventsPerPartition: Long,
                           myPartitions: Vector[Int],
                           producerIndex: Int
                         ): Unit = {

    if USE_LOG_FILE then
      outputLog.info(
        s"Producer #$producerIndex is starting with " +
          s"partitions: ${myPartitions.mkString(",")}, " +
          s"eventsPerPartition: $eventsPerPartition, " +
          s"sleepTimeMs: $sleepTimeMs" +
          s", max events per second: ${computeMaximumEventsPerSecond(nrOfKafkaPartitions(), holon.core.Config.PRODUCER_COUNT, eventsPerPartition, sleepTimeMs)}" +
          s", kafkaHost: $kafkaHost, kafkaPort: $kafkaPort"
      )

    val producer = holon.streaming.messaging.KafkaLogProducer(kafkaHost, kafkaPort, KAFKA_TOPIC_INPUT)
    val iter = holon.examples.nexmark.data.Nexmark.iterator()

    // track per‐window event counts for throughput logging:
    val inputEventsPerWindow = mutable.Map.empty[Long, Long]
    // track per‐second production counts (for external aggregation/monitoring):

    val startTimeMs = System.currentTimeMillis()
    var producedInWindow = 0L
    var windowStartMs = startTimeMs

    logger.debug(
      s"Producer #${producerIndex} starting with partitions: ${myPartitions.mkString(",")}"
    )
    var currentMultiplier = 0L
    var currentBatchSize = eventsPerPartition

    while true do

      // used for max throughput experiments.
      if (RUN_MAX_THROUGHPUT_PRODUCER) {
        val elapsedSeconds = (System.currentTimeMillis() - startTimeMs) / 1000
        val newMultiplier = (elapsedSeconds / 30).toInt

        if newMultiplier > currentMultiplier then
          currentMultiplier = newMultiplier
          currentBatchSize = eventsPerPartition * math.pow(2, currentMultiplier.toDouble).toInt
          if (USE_LOG_FILE) outputLog.info(s"[BatchSize Update] At ${elapsedSeconds}s: batch size doubled to $currentBatchSize events per partition") else logger.info(s"[BatchSize Update] At ${elapsedSeconds}s: batch size doubled to $currentBatchSize events per partition")
      }

      if (RUN_FLINK_PRODUCER) {
        for partitionId <- myPartitions do {
          val batch = (0L until currentBatchSize)
            .map(_ => iter.next())
            .map(x => {
              val window = defineWindow(x.timestamp)
              inputEventsPerWindow(window) = inputEventsPerWindow.getOrElse(window, 0L) + 1
              val key = writeBinary(partitionId)
              val value = write(x) // Serialize the event as JSON
              (key, value.getBytes("UTF-8"))
            })

          producer.send(batch)
          producedInWindow += currentBatchSize
        }
      }
      else {
        for partitionId <- myPartitions do {
          val batch = (0L until currentBatchSize)
            .map(_ =>
              val event = iter.next()
              val window = defineWindow(event.timestamp)
              inputEventsPerWindow(window) = inputEventsPerWindow.getOrElse(window, 0L) + 1
              (writeBinary(partitionId), holon.examples.nexmark.data.Nexmark.serialize(event))
            )

          producer.send(batch)
          producedInWindow += currentBatchSize
        }
      }

      // flush all outstanding writes:
      producer.flush()

      // log per‐second production rate (including producerIndex):
      val nowMs = System.currentTimeMillis()
      if (nowMs - windowStartMs >= 1000) {
        val elapsedWindowMs = nowMs - windowStartMs
        val countThisSec = producedInWindow
        val secondKey = windowStartMs / 1000

        val msg =
          s"[ProducerRate] producer $producerIndex produced $countThisSec events " +
            s"in last ${elapsedWindowMs}ms (partitions=${myPartitions.size}), second: $secondKey"

        if (USE_LOG_FILE) outputLog.info(msg) else logger.info(msg)

        producedInWindow = 0
        windowStartMs += 1000
      }

      // 4) Log per‐window throughput:
      val currentMaxWindow =
        if (inputEventsPerWindow.nonEmpty) inputEventsPerWindow.keys.max else -1L

      for ((k, v) <- inputEventsPerWindow if k < currentMaxWindow) {
        val msg = s"[Throughput] window: $k, eventCount: $v"
        if (USE_LOG_FILE) outputLog.info(msg) else logger.info(msg)
        inputEventsPerWindow -= k
      }

      // 5) Sleep until next tick:
      Thread.sleep(sleepTimeMs)
  }

  private def defineWindow(eventTime: Long): Long = {
    val time = eventTime / 10
    if (time % WINDOW_LENGTH == 0) time / WINDOW_LENGTH
    else (time / WINDOW_LENGTH) + 1
  }
}
