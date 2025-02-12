package holon.backend

import holon.*
import holon.Utils.*
import holon.backend.GCSClient.{bucketName, downloadStringFromBucket}
import upickle.default.*

import java.util.Base64
import java.util.concurrent.ConcurrentLinkedQueue

class Recovery(number: Int) {

    private val nodeNr = number
    private val consumers = scala.collection.mutable.Map.empty[Byte, LogConsumer]
    private val producers = scala.collection.mutable.Map.empty[Byte, LogProducer]
    private var procFun: ProcFun = null
    private val out = OutputCollectorImpl(producers)
    private val queue = new ConcurrentLinkedQueue[Job]()
    private val logger = Logger.apply("Recovery")

    Logger.setLevel("Recovery", "INFO")

    // Checkpoint interval in milliseconds
    private val checkpointInterval = 2_000L

    RunThread(this.run())

    def submitOrUpdate(job: Job): Unit = {
        this.queue.add(job)
    }

    private def setup(job: Job): Unit = {
        logger.info(s"Setting up job: $nodeNr")
        // setup consumers
        this.consumers.clear()
        job.consumers.foreach: ref =>
            val consumer = KafkaLogConsumer.fromRef(ref)
            this.consumers.put(ref.chn, consumer)

        // setup producers
        this.producers.clear()
        job.producers.foreach: ref =>
            val producer = KafkaLogProducer.fromRef(ref)
            this.producers.put(ref.chn, producer)

        // setup procFun
        this.procFun = job.procFun

        // Recover from the last snapshot
        if (GCSClient.checkIfFileExists(GCSClient.bucketName, "node" + nodeNr)) {
            logger.debug(s"Restoring snapshot for Node $nodeNr")
            restoreSnapshot()
        }
    }

    private def run(): Unit = {
        var time = 0L
        var checkpointTime = System.currentTimeMillis()

        while true do
            // check the job queue every 1_000 milliseconds
            val t = System.currentTimeMillis()
            if (t - time) > 1_000 then
                time = t
                checkJobQueue()

            // Take a snapshot after every checkpoint interval
            if this.procFun != null && (t - checkpointTime) > checkpointInterval then
                logger.debug(s"Checkpointing Node $nodeNr")
                val snapshot = this.procFun.snapshot()
                safeSnapshot(snapshot)
                checkpointTime = System.currentTimeMillis()

            runStep()
    }

    private inline def checkJobQueue(): Unit = {
        val job = this.queue.poll()
        if job != null then this.setup(job)
    }

    private inline def runStep(): Unit = {
        // 1. Poll, process each consumer
        for ((chn, consumer) <- consumers) {
            val records = consumer.poll()
            if !records.isEmpty then procFun.process(outputFunction, chn, records)
        }

        // 2. Flush all producers
        for ((chn, producer) <- producers) do producer.flush()
    }

    // Callback function for the processor function
    def outputFunction(chn: Byte, recs: LogProducerRecords): Unit = {
        out.collect(chn, recs)
    }

    /**
     * Safe snapshot of current state to Google Cloud Storage
     * Format: {channel: [partition,offset]}snapshot
     * E.g. {0:[1,1110];1:[0,4]}snapshot
     */
    private def safeSnapshot(snapshot: Array[Byte]): Unit = {
        val objectName = "node" + nodeNr

        val offsetsPerChannel = scala.collection.mutable.Map.empty[Byte, Iterable[(Int, Long)]]
        for ((chn, consumer) <- consumers) {
            val offsets = consumer.offsets()
            offsetsPerChannel.put(chn, offsets)
        }

        // Build snapshot string representation in format: {channel: [partition,offset]}snapshot
        val stringBuilder = new StringBuilder("{")
        for ((chn, offsets) <- offsetsPerChannel) {
            stringBuilder.append(s"$chn:")
            val firstOffset = offsets.head
            stringBuilder.append(s"[${firstOffset._1},${firstOffset._2}];")
        }
        // Remove the trailing comma
        if (stringBuilder.last == ';') stringBuilder.setLength(stringBuilder.length - 1)

        val base64EncodedSnapshot = Base64.getEncoder.encodeToString(snapshot)
        stringBuilder.append(s"}$base64EncodedSnapshot")
        val content = stringBuilder.toString()
        logger.debug(s"Snapshot for Node $nodeNr: $content")

        // Get GCSUploader object
        GCSClient.uploadStringToBucket(GCSClient.bucketName, objectName, content)
    }

    private def restoreSnapshot(): Unit = {
        val snapshotString = GCSClient.downloadStringFromBucket(GCSClient.bucketName, "node" + nodeNr)

        // Get string after closing }
        val snapshotBase64Encoded = snapshotString.split("}")(1)

        // Get content between { } and split on ;
        val contentBetweenBraces = snapshotString.substring(snapshotString.indexOf("{") + 1, snapshotString.indexOf("}"))

        // Split the content on ;
        val offsetsPerChannel = contentBetweenBraces.split(";").map { pair =>
            val Array(key, value) = pair.split(":")
            val values = value.stripPrefix("[").stripSuffix("]").split(",").map(_.toInt)
            key.toInt -> values
        }.toMap

        // Restore the state from the snapshot
        this.procFun.restore(Base64.getDecoder.decode(snapshotBase64Encoded))
        logger.debug(s"Restored snapshot for Node $nodeNr: $snapshotString")

        // Restore the offsets
        for ((chn, offsets) <- offsetsPerChannel) {
            val consumer = this.consumers(chn.toByte)

            // Restore the offsets (partition, offset)
            consumer.seek(offsets(0), offsets(1))
        }
    }

}
