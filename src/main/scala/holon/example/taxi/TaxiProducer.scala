package holon.example.taxi

import holon.*
import holon.backend.*
import holon.backend.kafka.KafkaLogProducer
import Config.*
import upickle.default.*

import scala.io.Source
import java.io.File
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.util.Locale
import scala.collection.mutable

object TaxiProducer {
    private val logger = Logger("TaxiProducer")
    Logger.setLevel("TaxiProducer", "INFO")

    private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US)

    private val reader = summon[ReadWriter[Events.TimeStampedEvent]]
    private val writer = summon[ReadWriter[Events.TimeStampedEvent]]

    inline def serialize(e: Events.TimeStampedEvent): Array[Byte] = writeBinary(e)(using writer)

    inline def deserialize(bytes: Array[Byte]): Events.TimeStampedEvent = readBinary(bytes)(using reader)

    object Events:
        case class TimeStampedEvent(
            event: TaxiTripEvent,
            timestamp: Long
        )derives ReadWriter

        sealed trait Event derives ReadWriter

        case class TaxiTripEvent(
            medallion: String,
            hackLicense: String,
            pickupTime: Long,
            dropoffTime: Long,
            tripTimeSecs: Int,
            tripDistance: Double,
            pickupLng: Double,
            pickupLat: Double,
            dropoffLng: Double,
            dropoffLat: Double,
            paymentType: String,
            fareAmount: Double,
            surcharge: Double,
            mtaTax: Double,
            tipAmount: Double,
            tollsAmount: Double,
            totalAmount: Double
        ) extends Event
            derives ReadWriter

//    given ReadWriter[TaxiTripEvent] = macroRW

    def main(args: Array[String]): Unit = {
        setupConfig()

        val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
        val host = kafkaBootstrapServers.split(":").head
        val port = kafkaBootstrapServers.split(":").last.toInt
        val PRODUCER_SLEEP_TIME_MS = sys.env.getOrElse("PRODUCER_SLEEP_TIME_MS", "100").toInt

//        while !holon.backend.cloud.FirestoreClient.isStartFlagSet do
//            logger.info("Waiting for start flag to be set.")
//            Thread.sleep(1000)
        Thread.sleep(2000)

        runProducer(host, port, PRODUCER_SLEEP_TIME_MS)
    }

    def runProducer(kafkaHost: String, kafkaPort: Int, sleepTime: Int): Unit = {
        val producer = KafkaLogProducer(kafkaHost, kafkaPort, KAFKA_TOPIC_INPUT)
        val fileIterator = taxiEventIterator("/Users/kolya/kth_projects/holon-streaming/taxi-data", 1)

        val inputEventsPerWindow = mutable.Map.empty[Long, Long]

        while fileIterator.hasNext do
            for i <- 0 until nrOfKafkaPartitions() do
                val batch = (0 until PRODUCER_BATCH_SIZE)
                    .flatMap(_ =>
                                 if (fileIterator.hasNext) {
                                     val event = fileIterator.next()
                                     val window = defineWindow(event.event.dropoffTime)
                                     inputEventsPerWindow(window) = inputEventsPerWindow.getOrElse(window, 0L) + 1
                                     Some((writeBinary(i), serialize(event)))
                                 } else None
                             )
                producer.send(batch)

            producer.flush()

            val currentMaxWindow = inputEventsPerWindow.keys.maxOption.getOrElse(0L)
            inputEventsPerWindow.keys
                .filter(_ < currentMaxWindow)
                .foreach(k =>
                             logger.info(s"[Throughput] window: $k, eventCount: ${inputEventsPerWindow(k)}")
                             inputEventsPerWindow -= k
                         )

            Thread.sleep(sleepTime)
    }

    def taxiEventIterator(dataDirPath: String, nrOfFiles: Int): Iterator[Events.TimeStampedEvent] = {
        val files = (1 to nrOfFiles).map(i => new File(s"$dataDirPath/taxi_data_$i.csv"))

        files.iterator.flatMap { file =>
            val source = Source.fromFile(file)("UTF-8")
            val lines = source.getLines().drop(1) // Skip header
            lines.flatMap(parseCsvLine)
        }
    }

    def parseCsvLine(line: String): Option[Events.TimeStampedEvent] = {
        val parts = line.split(",", -1)
        if (parts.length != 17) return None
        try {
            val pickupTime = LocalDateTime.parse(parts(2), formatter).atZone(java.time.ZoneId.of("UTC")).toInstant.toEpochMilli
            val dropoffTime = LocalDateTime.parse(parts(3), formatter).atZone(java.time.ZoneId.of("UTC")).toInstant.toEpochMilli
            Some(Events.TimeStampedEvent(
                Events.TaxiTripEvent(
                    parts(0), parts(1),
                    pickupTime, dropoffTime,
                    parts(4).toInt,
                    parts(5).toDouble,
                    parts(6).toDouble, parts(7).toDouble,
                    parts(8).toDouble, parts(9).toDouble,
                    parts(10),
                    parts(11).toDouble,
                    parts(12).toDouble,
                    parts(13).toDouble,
                    parts(14).toDouble,
                    parts(15).toDouble,
                    parts(16).toDouble
                    ),
                dropoffTime
                ))
        } catch {
            case _: Exception => None
        }
    }

    def setupConfig(): Unit = {
        val N_NODES = sys.env.getOrElse("N_NODES", "2").toInt
        Config.N_NODES = N_NODES
        val PARTITIONS_PER_NODE = sys.env.getOrElse("PARTITIONS_PER_NODE", "2").toInt
        Config.PARTITIONS_PER_NODE = PARTITIONS_PER_NODE
        val WINDOW_L = sys.env.getOrElse("WINDOW_LENGTH", "10000").toLong
        Config.WINDOW_LENGTH = WINDOW_L
    }

    def defineWindow(eventTime: Long): Long = {
        val startTime = LocalDateTime.parse("2013-01-01T00:00:00").atZone(java.time.ZoneId.of("UTC")).toInstant.toEpochMilli
        val windowSizeMillis = 15 * 60 * 1000 // 15 minutes in milliseconds
        (eventTime - startTime) / windowSizeMillis
    }
}
