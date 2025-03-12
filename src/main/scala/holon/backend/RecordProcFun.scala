package holon.backend

import org.apache.pekko.cluster.ddata.GCounter
import upickle.default.*
import holon.*
import holon.example.nexmark.Config.*
import holon.example.CRDT.*
import holon.example.Nexmark

class RecordProcFun(partition: Int) extends ProcFun {
    private var bidsCRDT = GCounter.empty
    private val partitionId = partition
    private val addr = address(partition)
    private val logger = Logger.apply("RecordProcFunction")

    Logger.setLevel("RecordProcFunction", "INFO")

    logger.info("Starting RecordProcFunction")

    override def process(
        outputFunction: (Int, Byte, LogProducerRecords) => Unit,
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
                            logger.debug(s"Received bid: $bidsCRDT")
                        case _ => () // ignore
                }
            case CHN_BROADCAST =>
                for (rec <- recs) {
                    crdtFromBinaryWithManifest(rec._2) match
                        case (Nexmark.BIDS_MANIFEST, delta) =>
                            bidsCRDT = bidsCRDT.mergeDelta(delta.asInstanceOf[GCounter])
                            logger.debug(s"Received broadcast: $bidsCRDT")
                        case _ => () // ignore
                }
            case _ =>
                throw new RuntimeException(s"Unknown channel: $chn")
        }

        // emit latest CRDT value
        outputFunction(partitionId, CHN_OUTPUT, Iterable.single((writeBinary(0), writeBinary(bidsCRDT.value))))

        // emit CRDT delta values
        if bidsCRDT.delta.isDefined then
            val delta = bidsCRDT.delta.get
            logger.debug(s"Broadcasting delta: $delta")
            outputFunction(partitionId, CHN_BROADCAST, Iterable.single((writeBinary(0), crdtToBinaryWithManifest(Nexmark.BIDS_MANIFEST, delta))))
            bidsCRDT = bidsCRDT.resetDelta
    }
    
    override def snapshot(): Array[Byte] = synchronized {
        logger.info(s"Partition $partitionId Snapshotting CRDT: $bidsCRDT")
        crdtToBinaryWithManifest(Nexmark.BIDS_MANIFEST, bidsCRDT)
    }
    
    override def restore(snapshot: Array[Byte]): Unit = {
        bidsCRDT = crdtFromBinaryWithManifest(snapshot)._2.asInstanceOf[GCounter]
        logger.info(s"Partition $partitionId Restored CRDT: $bidsCRDT")
    }

}
