package holon

import holon.Config.*
import holon.Utils.RunThreadWithTimeLimit
import holon.backend.*
import holon.backend.kafka.*
import holon.crdt.BidCountGCounterWrapper
import holon.example.Nexmark
import org.apache.pekko.cluster.ddata.GCounter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import upickle.legacy.*
import holon.serialization.*
import holon.example.nexmark.procfuns.BidCountProcessFun

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.*

class EndToEndControlTests extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

    val kafkaSystem: KafkaSystem = KafkaSystem(nrOfKafkaPartitions(), KAFKA_HOST, KAFKA_PORT)

    override def beforeAll(): Unit = {
    }

    override def beforeEach(): Unit = {
        deleteSnapshotFiles()
        // Restart topics
        kafkaSystem.startStream(KAFKA_TOPIC_INPUT)
        kafkaSystem.startStream(KAFKA_TOPIC_BROADCAST)
        kafkaSystem.startStream(KAFKA_TOPIC_CONTROL)
        kafkaSystem.startStream(KAFKA_TOPIC_OUTPUT)
    }

    override def afterEach(): Unit = {
        println("Deleting all Kafka topics...")
        kafkaSystem.deleteTopics(List(KAFKA_TOPIC_INPUT, KAFKA_TOPIC_BROADCAST, KAFKA_TOPIC_CONTROL, KAFKA_TOPIC_OUTPUT))
    }

    override def afterAll(): Unit = {
        // Clean up resources
        println("Cleaning up integration test environment...")
    }

    it should "verify that a Holon cluster with 2 nodes produces output to the output consumer" in {
        N_NODES = 2
        PARTITIONS_PER_NODE = 2

        val outputConsumer = setupOutputConsumer()
        var nrOutputs = 0
        val callbackFunction: () => Unit = () => nrOutputs += 1

        val RUNTIME = 15_000
        RunThreadWithTimeLimit(runNexmarkProducer(), RUNTIME)
        RunThreadWithTimeLimit(runOutputConsumer(outputConsumer, callbackFunction), RUNTIME)

        val j1 = job(List(0, 1))
        val holon1 = Holon(0)
        holon1.submitOrUpdateJob(j1)

        val j2 = job(List(2, 3))
        val holon2 = Holon(1)
        holon2.submitOrUpdateJob(j2)

        Thread.sleep(RUNTIME)
        stopHolonNodes(List(holon1, holon2))

        assert(nrOutputs > 0, "Output consumer should have received messages")
    }

    it should "verify that a Holon cluster with 3 nodes produces output to the output consumer" in {
        N_NODES = 3
        PARTITIONS_PER_NODE = 2

        val outputConsumer = setupOutputConsumer()
        var nrOutputs = 0
        val callbackFunction: () => Unit = () => nrOutputs += 1

        val RUNTIME = 40_000
        RunThreadWithTimeLimit(runNexmarkProducer(), RUNTIME)
        RunThreadWithTimeLimit(runOutputConsumer(outputConsumer, callbackFunction), RUNTIME)

        val j1 = job(List(0, 1))
        val holon1 = Holon(0)
        holon1.submitOrUpdateJob(j1)

        val j2 = job(List(2, 3))
        val holon2 = Holon(1)
        holon2.submitOrUpdateJob(j2)

        val j3 = job(List(4, 5))
        val holon3 = Holon(2)
        holon3.submitOrUpdateJob(j3)

        Thread.sleep(RUNTIME)
        stopHolonNodes(List(holon1, holon2, holon3))

        assert(nrOutputs > 0, "Output consumer should have received messages")
    }

    it should "verify that a Holon cluster with 5+ nodes produces output to the output consumer" in {
        N_NODES = 6
        PARTITIONS_PER_NODE = 4
        PRODUCER_BATCH_SIZE = 128

        val outputConsumer = setupOutputConsumer()
        var nrOutputs = 0
        val callbackFunction: () => Unit = () => nrOutputs += 1

        val RUNTIME = 30_000
        RunThreadWithTimeLimit(runNexmarkProducer(), RUNTIME)
        RunThreadWithTimeLimit(runNexmarkProducer(), RUNTIME)
        RunThreadWithTimeLimit(runNexmarkProducer(), RUNTIME)
        RunThreadWithTimeLimit(runNexmarkProducer(), RUNTIME)
        RunThreadWithTimeLimit(runOutputConsumer(outputConsumer, callbackFunction), RUNTIME)

        val nodesList = scala.collection.mutable.ListBuffer[Holon]()

        for (i <- 0 until N_NODES) {
            val partitions = (i * PARTITIONS_PER_NODE until (i + 1) * PARTITIONS_PER_NODE).toList
            val j = job(partitions)
            val holon = Holon(i)
            holon.submitOrUpdateJob(j)
            nodesList += holon
        }

        print("Nodes list: " + nodesList.toList)

        Thread.sleep(RUNTIME)
        stopHolonNodes(nodesList.toList)

        assert(nrOutputs > 0, "Output consumer should have received messages")
    }

    it should "verify that a Holon cluster continuous processing after one node fails" in {
        N_NODES = 2
        PARTITIONS_PER_NODE = 2

        val outputConsumer = setupOutputConsumer()
        var nrOutputs = 0
        val callbackFunction: () => Unit = () => nrOutputs += 1

        val RUNTIME = 22_000
        RunThreadWithTimeLimit(runNexmarkProducer(), RUNTIME)
        RunThreadWithTimeLimit(runOutputConsumer(outputConsumer, callbackFunction), RUNTIME)

        val j1 = job(List(0, 1))
        val holon1 = Holon(0)
        holon1.submitOrUpdateJob(j1)

        val j2 = job(List(2, 3))
        val holon2 = Holon(1)
        holon2.submitOrUpdateJob(j2)

        Thread.sleep(10_000)
        stopHolonNodes(List(holon2)) // Simulate failure of node 2
        nrOutputs = 0 // Reset output count

        Thread.sleep(12_000)
        val node1Partitions = holon1.partitions()

        assert(nrOutputs > 0, "Output consumer should have received messages after node failure")
        assert(node1Partitions.size == 4, "Node 1 should have all 4 partitions")
        assert(node1Partitions.sorted.equals(List(0, 1, 2, 3)), "Node 1 should have partitions 0,1,2,3")

        stopHolonNodes(List(holon1))
    }

    it should "verify that recovered Holon nodes get reintegrated into the cluster" in {
        N_NODES = 2
        PARTITIONS_PER_NODE = 2

        val outputConsumer = setupOutputConsumer()
        var nrOutputs = 0
        val callbackFunction: () => Unit = () => nrOutputs += 1

        val RUNTIME = 25_000
        RunThreadWithTimeLimit(runNexmarkProducer(), RUNTIME)
        RunThreadWithTimeLimit(runOutputConsumer(outputConsumer, callbackFunction), RUNTIME)

        val j1 = job(List(0, 1))
        val holon1 = Holon(0)
        holon1.submitOrUpdateJob(j1)

        val j2 = job(List(2, 3))
        val holon2 = Holon(1)
        holon2.submitOrUpdateJob(j2)

        Thread.sleep(5_000)
        stopHolonNodes(List(holon2)) // Simulate failure of node 2
        nrOutputs = 0 // Reset output count

        Thread.sleep(10_000)
        var node1Partitions = holon1.partitions()

        assert(nrOutputs > 0, "Output consumer should have received messages after node failure")
        assert(node1Partitions.size == 4, "Node 1 should have all 4 partitions")
        assert(node1Partitions.sorted.equals(List(0, 1, 2, 3)), "Node 1 should have partitions 0,1,2,3")

        val holon2Recovered = Holon(1)
        holon2Recovered.submitOrUpdateJob(j2) // Reintegrate node 2 into the cluster
        nrOutputs = 0

        Thread.sleep(10_000)

        node1Partitions = holon1.partitions()
        val node2Partitions = holon2Recovered.partitions()

        assert(nrOutputs > 0, "Output consumer should have received messages after node failure")
        assert(node1Partitions.nonEmpty, "Node 1 should own some partitions")
        assert(node2Partitions.nonEmpty, "Node 2 should own some partitions")

        stopHolonNodes(List(holon1, holon2Recovered))
    }

    it should "verify that idle nodes work steal partitions from overloaded nodes" in {
        N_NODES = 2
        PARTITIONS_PER_NODE = 2
        SLEEP_BETWEEN_POLLS = 200L

        val outputConsumer = setupOutputConsumer()
        var nrOutputs = 0
        val callbackFunction: () => Unit = () => nrOutputs += 1

        val RUNTIME = 40_000
        RunThreadWithTimeLimit(runNexmarkProducerUnevenWorkload(), RUNTIME)
        RunThreadWithTimeLimit(runNexmarkProducerUnevenWorkload(), RUNTIME)
        RunThreadWithTimeLimit(runNexmarkProducerUnevenWorkload(), RUNTIME)
        RunThreadWithTimeLimit(runNexmarkProducerUnevenWorkload(), RUNTIME)
        RunThreadWithTimeLimit(runNexmarkProducerUnevenWorkload(), RUNTIME)
        RunThreadWithTimeLimit(runOutputConsumer(outputConsumer, callbackFunction), RUNTIME)

        val j1 = job(List(0, 1))
        val holon1 = Holon(0)
        holon1.submitOrUpdateJob(j1)

        val j2 = job(List(2, 3))
        val holon2 = Holon(1)
        holon2.submitOrUpdateJob(j2)

        Thread.sleep(RUNTIME)

        val n1Partitions = holon1.partitions()
        val n2Partitions = holon2.partitions()

        //        assert(nrOutputs > 0, "Output consumer should have received messages.")
        assert(n1Partitions.size == 3, "Node 1 should have 3 partitions. It should have stolen one!")
        assert(n2Partitions.size == 1, "Node 2 should only have one partition remaining after work stealing.")

        holon2.stop()
        holon1.stop()
    }

    it should "verify that each produced record is processed exactly-once" in {
        N_NODES = 2
        PARTITIONS_PER_NODE = 2
        PRODUCER_BATCH_SIZE = 1000

        val outputConsumer = setupOutputConsumer()
        var nrOutputs = 0
        // Create window set
        val windowSet = scala.collection.mutable.Set[Long]()
        val outputCallbackFunction: (Int, Int) => Unit = (recordCount: Int, window: Int) => {
            // Deduplication logic
            if (!windowSet.contains(window)) {
                windowSet.add(window)
                nrOutputs += recordCount
            }
        }
        var nrProducedRecords = 0
        val producerCallback: (Int) => Unit = (recordCount: Int) => {
            nrProducedRecords += recordCount
        }

        val RUNTIME = 15_000
        RunThreadWithTimeLimit(runNexmarkProducerWithRecordCount(5_000, producerCallback), 10_000)
        RunThreadWithTimeLimit(runOutputConsumerWithRecordCount(outputConsumer, outputCallbackFunction), RUNTIME)

        val j1 = job(List(0, 1))
        val holon1 = Holon(0)
        holon1.submitOrUpdateJob(j1)

        val j2 = job(List(2, 3))
        val holon2 = Holon(1)
        holon2.submitOrUpdateJob(j2)

        Thread.sleep(RUNTIME)
        stopHolonNodes(List(holon1, holon2))

        assert(nrOutputs > 0, "Output consumer should have received messages")
        assert(nrProducedRecords > 0, "Producer should have produced messages")
        assert(nrProducedRecords == nrOutputs, "Output consumer should have received the same number of messages as produced")
    }

    it should "verify that each produced record is processed exactly-once with node failure" in {
        N_NODES = 3
        PARTITIONS_PER_NODE = 2
        PRODUCER_BATCH_SIZE = 1000
        CHECKPOINT_INTERVAL = 5_000L

        val outputConsumer = setupOutputConsumer()
        var nrOutputs = 0
        // Create window set
        val windowSet = scala.collection.mutable.Set[Long]()
        val outputCallbackFunction: (Int, Int) => Unit = (recordCount: Int, window: Int) => {
            // Deduplication logic
            if (!windowSet.contains(window)) {
                windowSet.add(window)
                nrOutputs += recordCount
            }
        }
        var nrProducedRecords = 0
        val producerCallback: (Int) => Unit = (recordCount: Int) => {
            nrProducedRecords += recordCount
        }

        val RUNTIME = 27_000
        val FAILURE_TIME = 7_000
        RunThreadWithTimeLimit(runNexmarkProducerWithRecordCount(12_000, producerCallback), 20_000)
        RunThreadWithTimeLimit(runOutputConsumerWithRecordCount(outputConsumer, outputCallbackFunction), RUNTIME)

        val j1 = job(List(0, 1))
        val holon1 = Holon(0)
        holon1.submitOrUpdateJob(j1)

        val j2 = job(List(2, 3))
        val holon2 = Holon(1)
        holon2.submitOrUpdateJob(j2)

        val j3 = job(List(4, 5))
        val holon3 = Holon(2)
        holon3.submitOrUpdateJob(j3)

        Thread.sleep(FAILURE_TIME)
        stopHolonNodes(List(holon2)) // Simulate failure of node 2

        Thread.sleep(RUNTIME - FAILURE_TIME)
        stopHolonNodes(List(holon1, holon3))

        assert(nrOutputs > 0, "Output consumer should have received messages")
        assert(nrProducedRecords > 0, "Producer should have produced messages")
        println(s"Produced records: $nrProducedRecords, Output records: $nrOutputs")
        assert(nrProducedRecords == nrOutputs, "Output consumer should have received the same number of messages as produced")
    }

    def runNexmarkProducer() = {
        val producer = KafkaLogProducer(KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC_INPUT)
        val iter = Nexmark.iterator()
        while true do
            for i <- 0 until nrOfKafkaPartitions() do
                val batch = (0 until PRODUCER_BATCH_SIZE).map(_ => iter.next()).map(x => (writeBinary(i), Nexmark.serialize(x)))
                producer.send(batch)

            producer.flush()
            Thread.sleep(PRODUCER_SLEEP_MS)
    }

    def runNexmarkProducerWithRecordCount(runtimeMs: Long, countCallback: (Int) => Unit) = {
        val producer = KafkaLogProducer(KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC_INPUT)
        val iter = Nexmark.iterator()
        val startTime = System.currentTimeMillis()

        var maxRecordTime = 0L

        while (System.currentTimeMillis() - startTime < runtimeMs) {
            for (i <- 0 until nrOfKafkaPartitions()) {
                var bidCount = 0
                val batch = (0 until PRODUCER_BATCH_SIZE).map(_ => iter.next()).map { x =>
                    x.event match {
                        case bid: Nexmark.Events.Bid =>
                            bidCount += 1
                        case _ =>
                            // Ignore other events
                    }

                    if (x.timestamp > maxRecordTime) {
                        maxRecordTime = x.timestamp
                    }

                    // Serialize the event and send it to the producer
                    (writeBinary(i), Nexmark.serialize(x))
                }
                producer.send(batch)
                countCallback(bidCount)
            }

            producer.flush()
            Thread.sleep(PRODUCER_SLEEP_MS)
        }

        // Create bids with timestamp in next window, to close all previous windows!
        val eventTimeNextWindow = maxRecordTime + (WINDOW_LENGTH * 10)
        while (true) {
            for i <- 0 until nrOfKafkaPartitions() do
                val bid = Nexmark.Events.Bid(
                    auction = 0,
                    bidder = 0,
                    price = 0,
                    dateTime = eventTimeNextWindow,
                    extra = ""
                    )
                val timestampedEvent = Nexmark.Events.TimeStampedEvent(bid, eventTimeNextWindow)


                val batch = (0 until 1).map(x => (writeBinary(i), Nexmark.serialize(timestampedEvent)))
                producer.send(batch)

            producer.flush()
            Thread.sleep(PRODUCER_SLEEP_MS)
        }
    }

    /**
     * Run the Nexmark producer with an uneven workload distribution.
     * This simulates a scenario where some partitions receive more data than others..
     */
    def runNexmarkProducerUnevenWorkload() = {
        val producer = KafkaLogProducer(KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC_INPUT)
        val iter = Nexmark.iterator()
        var index = 0
        while true do
            val targetPartition = if (index < 5) {
                scala.util.Random.shuffle(List(0, 1)).head
            } else {
                scala.util.Random.shuffle(List(2, 3)).head
            }
            val batch = (0 until PRODUCER_BATCH_SIZE).map(_ => iter.next()).map(x => (writeBinary(targetPartition), Nexmark.serialize(x)))
            producer.send(batch)
            index += 1

        producer.flush()
        Thread.sleep(0)
    }

    def setupOutputConsumer(): KafkaLogConsumer = {
        KafkaLogConsumer(KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC_OUTPUT, (0 until nrOfKafkaPartitions()).toList)
    }

    def runOutputConsumer(outputConsumer: KafkaLogConsumer, callbackFunction: () => Unit) = {
        val logger = Logger.apply("Consumer")
        Logger.setLevel("Consumer", "INFO")
        logger.info("Starting Output Consumer")
        while true do
            outputConsumer.poll() match
                case Nil =>
                    Thread.sleep(CONSUMER_SLEEP_MS)
                case records =>
                    records.foreach: r =>
                        logger.info(s"[OUTPUT]: Received record!")
                        callbackFunction() // Example usage of the callback function
    }

    def runOutputConsumerWithRecordCount(outputConsumer: KafkaLogConsumer, countCallback: (Int, Int) => Unit) = {
        val logger = Logger.apply("Consumer")
        Logger.setLevel("Consumer", "INFO")
        logger.info("Starting Output Consumer")
        while true do
            outputConsumer.poll() match
                case Nil =>
                    Thread.sleep(CONSUMER_SLEEP_MS)
                case records =>
                    records.foreach: r =>
                        val outputState = readBinary[OutputState](r._2)
                        val partition = outputState.partition
                        val windowId = outputState.window
                        val outputValue = outputState.value
                        logger.info(s"[OUTPUT]: Received records for window $windowId in partition $partition, value: $outputValue")
                        countCallback(outputValue.toInt, windowId.toInt)
    }

    def setupKafka(): Unit = {
        val logger = Logger.apply("Kafka")
        Logger.setLevel("Kafka", "ERROR")
        logger.info("Setting up Kafka")

        val system = KafkaSystem(nrOfKafkaPartitions(), KAFKA_HOST, KAFKA_PORT)
        system.startStream(KAFKA_TOPIC_INPUT)
        system.startStream(KAFKA_TOPIC_BROADCAST)
        system.startStream(KAFKA_TOPIC_CONTROL)
        system.startStream(KAFKA_TOPIC_OUTPUT)

        logger.info("Kafka setup complete")
    }

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

    // Job function creates a job object with the specified consumers and producers
    def job(partitions: List[Int]): Job = {
        val nexmarkConsumers = partitions.map { partition =>
            consumerRef(CHN_INPUT, KAFKA_TOPIC_INPUT, partition)
        }
        val internalConsumers = List(
            consumerRef(CHN_BROADCAST, KAFKA_TOPIC_BROADCAST, 0),
            consumerRef(CHN_CONTROL, KAFKA_TOPIC_CONTROL, 0)
            )
        val consumers = nexmarkConsumers ++ internalConsumers

        val producers = List(
            producerRef(CHN_INPUT, KAFKA_TOPIC_INPUT),
            producerRef(CHN_BROADCAST, KAFKA_TOPIC_BROADCAST),
            producerRef(CHN_CONTROL, KAFKA_TOPIC_CONTROL),
            producerRef(CHN_OUTPUT, KAFKA_TOPIC_OUTPUT),
            )

        val job = Job(
            consumers = consumers,
            producers = producers,
            procFunFactory = new BidCountProcFunFactory(),
            partitions = partitions,
            )

        job
    }

    def deleteSnapshotFiles(): Unit = {
        val snapshotsPath = Paths.get("snapshots")
        if (Files.exists(snapshotsPath) && Files.isDirectory(snapshotsPath)) {
            Files.list(snapshotsPath).iterator().asScala.foreach { file =>
                Files.deleteIfExists(file)
            }
            println("All files in the 'snapshots' folder have been deleted.")
        } else {
            println("'snapshots' folder does not exist or is not a directory.")
        }
    }

    def stopHolonNodes(holonNodes: List[Holon]): Unit = {
        try {
            holonNodes.foreach(_.stop())
        } catch {
            case e: Exception =>
                println("Error stopping Holon nodes: " + e.getMessage)
        }
    }

    /**
     * Counts the number of bids in a window.
     */
    class BidCountProcFunFactory extends ProcFunFactory {
        override def create(partition: Int): ProcFun = {
            val crdts = List(WindowedRecordProcFun(BidCountGCounterWrapper, partition, 0, gcounterRW))
            new BidCountProcessFun(partition, crdts)
        }
    }
}
