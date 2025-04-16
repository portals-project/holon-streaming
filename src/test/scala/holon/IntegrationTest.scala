package holon

import holon.*
import holon.Utils.{RunThread, RunThreadWithTimeLimit}
import holon.backend.*
import holon.example.Nexmark
import holon.Config.*
import holon.example.nexmark.BidCountWindowedFactory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import upickle.default.*

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.*

class IntegrationTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

    val kafkaSystem: KafkaSystem = KafkaSystem(KAFKA_N_PARTITIONS, KAFKA_HOST, KAFKA_PORT)

    override def beforeAll(): Unit = {
    }

    override def beforeEach(): Unit = {
        deleteSnapshotFiles()
        // Restart topics
        kafkaSystem.startStream(KAFKA_TOPIC_NEXMARK)
        kafkaSystem.startStream(KAFKA_TOPIC_BROADCAST)
        kafkaSystem.startStream(KAFKA_TOPIC_CONTROL)
        kafkaSystem.startStream(KAFKA_TOPIC_OUTPUT)
    }

    override def afterEach(): Unit = {
        println("Deleting all Kafka topics...")
        kafkaSystem.deleteTopics(List(KAFKA_TOPIC_NEXMARK, KAFKA_TOPIC_BROADCAST, KAFKA_TOPIC_CONTROL, KAFKA_TOPIC_OUTPUT))
    }

    override def afterAll(): Unit = {
        // Clean up resources
        println("Cleaning up integration test environment...")
    }

    //    "MyService" should "interact with the database correctly" in {
    //        // Example test logic
    //        val result = MyService.performAction("testInput")
    //        result shouldBe "expectedOutput"
    //    }
    //
    //    it should "handle external API calls" in {
    //        // Example test logic for API interaction
    //        val apiResponse = MyService.callExternalApi("testData")
    //        apiResponse should include("success")
    //    }

    it should "verify that a Holon cluster with 2 nodes produces output to the output consumer" in {
        // Set up the output collector to capture output
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
        holon1.submitOrUpdate(j1)

        val j2 = job(List(2, 3))
        val holon2 = Holon(1)
        holon2.submitOrUpdate(j2)

        Thread.sleep(RUNTIME)
        holon1.stop()
        holon2.stop()

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
        holon1.submitOrUpdate(j1)

        val j2 = job(List(2, 3))
        val holon2 = Holon(1)
        holon2.submitOrUpdate(j2)

        Thread.sleep(10_000)
        holon2.stop() // Simulate failure of node 2
        nrOutputs = 0 // Reset output count

        Thread.sleep(12_000)
        val node1Partitions = holon1.partitions()
        holon1.stop()

        assert(nrOutputs > 0, "Output consumer should have received messages after node failure")
        assert(node1Partitions.size == 4, "Node 1 should have all 4 partitions")
        assert(node1Partitions.sorted.equals(List(0,1,2,3)), "Node 1 should have partitions 0,1,2,3")
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
        holon1.submitOrUpdate(j1)

        val j2 = job(List(2, 3))
        val holon2 = Holon(1)
        holon2.submitOrUpdate(j2)

        Thread.sleep(5_000)
        holon2.stop() // Simulate failure of node 2
        nrOutputs = 0 // Reset output count

        Thread.sleep(10_000)
        var node1Partitions = holon1.partitions()

        assert(nrOutputs > 0, "Output consumer should have received messages after node failure")
        assert(node1Partitions.size == 4, "Node 1 should have all 4 partitions")
        assert(node1Partitions.sorted.equals(List(0, 1, 2, 3)), "Node 1 should have partitions 0,1,2,3")

        val holon2Recovered = Holon(1)
        holon2Recovered.submitOrUpdate(j2) // Reintegrate node 2 into the cluster
        nrOutputs = 0

        Thread.sleep(10_000)

        node1Partitions = holon1.partitions()
        holon1.stop()
        val node2Partitions = holon2Recovered.partitions()
        holon2Recovered.stop()

        assert(nrOutputs > 0, "Output consumer should have received messages after node failure")
        assert(node1Partitions.nonEmpty, "Node 1 should own some partitions")
        assert(node2Partitions.nonEmpty, "Node 2 should own some partitions")
    }

    it should "verify that idle nodes work steal partitions from overloaded nodes" in {
        N_NODES = 2
        PARTITIONS_PER_NODE = 2
        SLEEP_BETWEEN_POLLS = 50L

        val outputConsumer = setupOutputConsumer()
        var nrOutputs = 0
        val callbackFunction: () => Unit = () => nrOutputs += 1

        val RUNTIME = 40_000
        RunThreadWithTimeLimit(runNexmarkProducerUnevenWorkload(), RUNTIME)
        RunThreadWithTimeLimit(runNexmarkProducerUnevenWorkload(), RUNTIME)
        RunThreadWithTimeLimit(runOutputConsumer(outputConsumer, callbackFunction), RUNTIME)

        val j1 = job(List(0, 1))
        val holon1 = Holon(0)
        holon1.
        holon1.submitOrUpdate(j1)

        val j2 = job(List(2, 3))
        val holon2 = Holon(1)
        holon2.submitOrUpdate(j2)

        Thread.sleep(RUNTIME)

        val n1Partitions = holon1.partitions()
        val n2Partitions = holon2.partitions()

//        assert(nrOutputs > 0, "Output consumer should have received messages.")
        assert(n1Partitions.size == 3, "Node 1 should have all 3 partitions. It should have stolen one!")
        assert(n2Partitions.size == 1, "Node 2 should only have one partition remaining after work stealing.")

        holon2.stop()
        holon1.stop()
    }

    def runNexmarkProducer() = {
        KAFKA_N_PARTITIONS = N_NODES * PARTITIONS_PER_NODE
        val producer = KafkaLogProducer(KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC_NEXMARK)
        val iter = Nexmark.iterator()
        while true do
            for i <- 0 until KAFKA_N_PARTITIONS do
                val batch = (0 until PRODUCER_BATCH_SIZE).map(_ => iter.next()).map(x => (writeBinary(i), Nexmark.serialize(x)))
                producer.send(batch)

            producer.flush()
            Thread.sleep(PRODUCER_SLEEP_MS)
    }

    /**
     * Run the Nexmark producer with an uneven workload distribution.
     * This simulates a scenario where some partitions receive more data than others.
     * 90% of the data goes to partitions 2 and 3, while 10% goes to partitions 0 and 1.
     */
    def runNexmarkProducerUnevenWorkload() = {
        KAFKA_N_PARTITIONS = N_NODES * PARTITIONS_PER_NODE
        val producer = KafkaLogProducer(KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC_NEXMARK)
        val iter = Nexmark.iterator()
        while true do
            for i <- 0 until KAFKA_N_PARTITIONS do
                val targetPartition =
                    if (i == 2 || i == 3 || scala.util.Random.nextDouble() < 0.95)
                        scala.util.Random.shuffle(List(2, 3)).head
                    else
                        scala.util.Random.shuffle(List(0, 1)).head
                val batch = (0 until PRODUCER_BATCH_SIZE).map(_ => iter.next()).map(x => (writeBinary(targetPartition), Nexmark.serialize(x)))
                producer.send(batch)

            producer.flush()
            Thread.sleep(PRODUCER_SLEEP_MS)
    }

    def setupOutputConsumer(): KafkaLogConsumer = {
        KafkaLogConsumer(KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC_OUTPUT, (0 until KAFKA_N_PARTITIONS).toList)
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
                        val crdtValue = readBinary[BigInt](r._2)
                        logger.info(s"[OUTPUT]:Closed a window with final aggregate: $crdtValue")
                        callbackFunction() // Example usage of the callback function
    }

    def setupKafka(): Unit = {
        val logger = Logger.apply("Kafka")
        Logger.setLevel("Kafka", "ERROR")
        logger.info("Setting up Kafka")

        val system = KafkaSystem(KAFKA_N_PARTITIONS, KAFKA_HOST, KAFKA_PORT)
        system.startStream(KAFKA_TOPIC_NEXMARK)
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
            consumerRef(CHN_NEXMARK, KAFKA_TOPIC_NEXMARK, partition)
        }
        val internalConsumers = List(
            consumerRef(CHN_BROADCAST, KAFKA_TOPIC_BROADCAST, 0),
            consumerRef(CHN_CONTROL, KAFKA_TOPIC_CONTROL, 0)
            )
        val consumers = nexmarkConsumers ++ internalConsumers

        val producers = List(
            producerRef(CHN_NEXMARK, KAFKA_TOPIC_NEXMARK),
            producerRef(CHN_BROADCAST, KAFKA_TOPIC_BROADCAST),
            producerRef(CHN_CONTROL, KAFKA_TOPIC_CONTROL),
            producerRef(CHN_OUTPUT, KAFKA_TOPIC_OUTPUT),
            )

        val job = Job(
            consumers = consumers,
            producers = producers,
            //      procFunFactory = new RecordProcFunFactory(),
            procFunFactory = new BidCountWindowedFactory(),
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
}
