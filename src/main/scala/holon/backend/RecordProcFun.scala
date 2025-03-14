package holon.backend

import org.apache.pekko.cluster.ddata.GCounter
import upickle.default.*
import holon.*
import holon.example.nexmark.Config.*
import holon.example.CRDT.*
import holon.example.Nexmark

/**
 * This ProcFun implementation processes incoming bid events from the Nexmark stream.
 * It maintains a GCounter to count the number of bids received.
 */
class RecordProcFun(partition: Int) extends ProcFun {
    // Local GCounter for counting the number of bids received.
    private var bidsCRDT = GCounter.empty
    // Address for the GCounter.
    private val addr = address(partition)

    // Emit frequency.
    private val emitInterval = 10_000
    private var lastEmitTime = System.currentTimeMillis()

    // Only broadcast every 30 messages.
    private var messageCount = 30


    private val logger = Logger.apply("RecordProcFunction")
    Logger.setLevel("RecordProcFunction", "INFO")
    logger.info("Starting RecordProcFunction")

    override def process(
        outputFunction: (Int, Byte, LogProducerRecords) => Unit,
        chn: Byte,
        recs: LogConsumerRecords,
    ): Unit = {
        // process inputs.
        // Every n seconds, emit the CRDT state to the output channel.
        if (System.currentTimeMillis() - lastEmitTime > emitInterval) {
            logger.debug(s"Emitting CRDT state to output channel")
            outputFunction(partition, CHN_OUTPUT, Iterable.single((writeBinary(0), writeBinary(bidsCRDT.value))))
            lastEmitTime = System.currentTimeMillis()
        }
        chn match {
            case CHN_NEXMARK =>
                for (rec <- recs) {
                    val event = Nexmark.deserialize(rec._2).event
                    event match {
                        case bid: Nexmark.Events.Bid =>
                            bidsCRDT = bidsCRDT.increment(addr, 1L)
                        case _ =>
                            //logger.debug(s"Ignored non-bid event: $event")
                    }
                }
            case CHN_BROADCAST =>
                for (rec <- recs) {
                    crdtFromBinaryWithManifest(rec._2) match {
                        case (Nexmark.BIDS_MANIFEST, state) =>
                            bidsCRDT = bidsCRDT.merge(state.asInstanceOf[GCounter])
                        case _ =>
                            logger.debug(s"Ignored broadcast with unknown manifest or invalid data ${rec._2.mkString("Array(", ", ", ")")}")
                    }
                }
            case _ =>
                throw new RuntimeException(s"Unknown channel: $chn")
        }
        // Emit CRDT state (GCounter) to the broadcast channel.

        // TODO find better solution
        messageCount += 1
        if (messageCount >= 30) {
            logger.debug(s"Partition $partition emitting CRDT state to broadcast channel")
            outputFunction(partition, CHN_BROADCAST, Iterable.single((writeBinary(0), crdtToBinaryWithManifest(Nexmark.BIDS_MANIFEST, bidsCRDT))))
            messageCount = 0
        }

    }

    override def snapshot(): Array[Byte] = {
        logger.debug("Taking snapshot")
        crdtToBinaryWithManifest(Nexmark.BIDS_MANIFEST, bidsCRDT)
    }

    override def restore(snapshot: Array[Byte]): Unit = {
        logger.debug("Restoring from snapshot")
        bidsCRDT = crdtFromBinaryWithManifest(snapshot)._2.asInstanceOf[GCounter]
        logger.info(s"Partition $partition Restored CRDT: $bidsCRDT")
    }

    override def defineWindow(eventTime: Long): Long = {
        0L
    }
}
