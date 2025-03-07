package holon.example.nexmark

import upickle.default.*

import org.apache.pekko.cluster.ddata.*

import holon.*
import holon.backend.*
import holon.example.nexmark.Config.*
import holon.example.CRDT.*
import holon.example.Nexmark
import holon.Utils.*

class QueryProcFun(partition: Int) extends ProcFun {
  private var bidsCRDT = GCounter.empty
  private val addr = address(partition)
  private val logger = Logger.apply("QueryProcFun")
  Logger.setLevel("QueryProcFun", "INFO")

  logger.info("Starting QueryProcFun")

  override def process(
      out: OutputCollector,
      chn: Byte,
      recs: LogConsumerRecords,
  ): Unit = {
    // process inputs
    chn match {
      case CHN_NEXMARK =>
        for (rec <- recs) {
          val event = Nexmark.deserialize(rec._2).event
          event match
            case Nexmark.Events.Bid(_, _, _, _, _) =>
              bidsCRDT = bidsCRDT.increment(addr, 1)
            case _ => () // ignore
        }
      case CHN_BROADCAST =>
        for (rec <- recs) {
          crdtFromBinaryWithManifest(rec._2) match
            case (Nexmark.BIDS_MANIFEST, delta) =>
              bidsCRDT = bidsCRDT.mergeDelta(delta.asInstanceOf[GCounter])
            case _ => () // ignore
        }
      case _ =>
        throw new RuntimeException(s"Unknown channel: $chn")
    }

    // emit latest CRDT value
    out.collect(CHN_OUTPUT, Iterable.single((writeBinary(0), writeBinary(bidsCRDT.value))))

    // emit CRDT delta values
    if bidsCRDT.delta.isDefined then
      val delta = bidsCRDT.delta.get
      out.collect(CHN_BROADCAST, Iterable.single((writeBinary(0), crdtToBinaryWithManifest(Nexmark.BIDS_MANIFEST, delta))))
      bidsCRDT = bidsCRDT.resetDelta
  }

  override def snapshot(): Array[Byte] = synchronized {
    logger.info(s"Partition Snapshotting CRDT: $bidsCRDT")
    val snap = crdtToBinaryWithManifest(Nexmark.SNAPSHOT_MANIFEST, bidsCRDT)
    logger.info(s"Snapshot: ${snap.mkString(",")}")
    val decoded = crdtFromBinaryWithManifest(snap)._2.asInstanceOf[GCounter]
    logger.info(s"Snapshot decoded $decoded")
    bidsCRDT = decoded
    logger.info(s"Restored CRDT: $bidsCRDT")
    logger.info(s"CRDT.value: ${bidsCRDT.value}")

    snap
  }

  override def restore(snapshot: Array[Byte]): Unit = {
    bidsCRDT = crdtFromBinaryWithManifest(snapshot)._2.asInstanceOf[GCounter]
  }
}

/** Count the total number of bids. */
object Query {
  Logger.setRootLevel("ERROR")

  private def consumerRef(chn: Byte, topic: String, partition: Int): ConsumerRef =
    ConsumerRef(
      chn = chn,
      host = KAFKA_HOST,
      port = KAFKA_PORT,
      topic = topic,
      partitions = List(partition),
    )

  private def producerRef(chn: Byte, topic: String): ProducerRef =
    ProducerRef(
      chn = chn,
      host = KAFKA_HOST,
      port = KAFKA_PORT,
      topic = topic,
    )

  def job(partition: Int): Job = {
    val consumers = List(
      consumerRef(CHN_NEXMARK, KAFKA_TOPIC_NEXMARK, partition),
      consumerRef(CHN_BROADCAST, KAFKA_TOPIC_BROADCAST, 0),
    )

    val producers = List(
      producerRef(CHN_NEXMARK, KAFKA_TOPIC_NEXMARK),
      producerRef(CHN_BROADCAST, KAFKA_TOPIC_BROADCAST),
      producerRef(CHN_OUTPUT, KAFKA_TOPIC_OUTPUT),
    )

    val job = Job(
      consumers = consumers,
      producers = producers,
      procFun = new QueryProcFun(partition),
    )

    job
  }

  def runNexmarkProducer() = {
    val producer = KafkaLogProducer(KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC_NEXMARK)
    val iter = Nexmark.iterator()
    while true do
      for i <- 0 until KAFKA_N_PARTITIONS do //
        val batch = (0 until PRODUCER_BATCH_SIZE).map(_ => iter.next()).map(x => (writeBinary(i), Nexmark.serialize(x)))
        producer.send(batch)
      producer.flush()
      Thread.sleep(PRODUCER_SLEEP_MS)
  }

  def runOutputConsumer() = {
    val logger = Logger.apply("Consumer")
    Logger.setLevel("Consumer", "INFO")
    val output = KafkaLogConsumer(KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC_OUTPUT, (0 until KAFKA_N_PARTITIONS).toList)
    while true do
      output.poll() match
        case Nil =>
          Thread.sleep(CONSUMER_SLEEP_MS)
        case records =>
          records.foreach: r =>
            val bids = readBinary[(Long)](r._2)
            logger.info(s"Bids: $bids")
  }

  def setupKafka(): Unit = {
    val system = KafkaSystem(KAFKA_N_PARTITIONS, KAFKA_HOST, KAFKA_PORT)
    system.startStream(KAFKA_TOPIC_NEXMARK)
    system.startStream(KAFKA_TOPIC_BROADCAST)
    system.startStream(KAFKA_TOPIC_OUTPUT)
  }

  def main(args: Array[String]): Unit = {
    SafeRun(10_000) {
      val logger = Logger.apply("Nexmark Query")
      Logger.setLevel("Nexmark Query", "INFO")
      logger.info("Starting Nexmark Query")

      setupKafka()

      RunThread(runNexmarkProducer())
      RunThread(runNexmarkProducer())
      RunThread(runNexmarkProducer())
      RunThread(runNexmarkProducer())
      RunThread(runNexmarkProducer())
      RunThread(runOutputConsumer())

      for (i <- 0 until KAFKA_N_PARTITIONS) {
        val j = job(i)
        val holon = Holon()
        holon.submitOrUpdate(j)
      }

      Thread.sleep(10_000)
    }
  }
}
