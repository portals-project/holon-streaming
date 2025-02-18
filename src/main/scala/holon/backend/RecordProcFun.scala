package holon.backend

import org.apache.pekko.cluster.ddata.{GCounter, GSet, LWWRegister, LWWRegisterKey}
import org.apache.pekko.cluster.ddata.SelfUniqueAddress
import upickle.default.*
import holon.*
import holon.example.nexmark.Config.*
import holon.example.CRDT.*
import holon.example.Nexmark
import scala.concurrent.duration._
import java.time.Instant

class RecordProcFun(partition: Int) extends ProcFun {
    private var bidsCRDT = GCounter.empty
    private var maxBidsCRDT = GSet.empty[(SelfUniqueAddress, Long)]


    private val addr = address(partition)
    private var curValue = Option[Long](0)

    // Init LWWRegister for max value tracking
    private var maxBidRegister = LWWRegister(addr, 0L)

    private val logger = Logger.apply("RecordProcFunction")
    Logger.setLevel("RecordProcFunction", "INFO")
    logger.info("Starting RecordProcFunction")

    override def process(
                          outputFunction: (Byte, LogProducerRecords) => Unit,
                          chn: Byte,
                          recs: LogConsumerRecords,
                        ): Unit = {
        // process inputs
        chn match {
            case CHN_NEXMARK =>
                for (rec <- recs) {
                    val event = Nexmark.deserialize(rec._2).event
                    event match {
                        case bid: Nexmark.Events.Bid =>
                            bidsCRDT = bidsCRDT.increment(addr, 1)

                            // Update LWWRegister with max value if needed
                            if (bid.price > maxBidRegister.value) {
                                val newPrice = bid.price
                                maxBidRegister = maxBidRegister.withValue(addr, newPrice)
                                logger.info(s"Updated max value from LWWRegister: ${maxBidRegister.updatedBy} with new value: $newPrice, LWWRegister value: ${maxBidRegister.value}")
                            }

                            // Extract max value safely
//                            val maxOpt = maxBidsCRDT.elements.map(_._2).maxOption
//                            logger.info(s"current max value: $maxOpt for CRDT: $maxBidsCRDT")
//                            maxOpt match {
//                                case Some(currentMax) =>
//                                    if (bid.price > currentMax) {
////                                        logger.info(s"Updating max value CRDT: $bid, current max: $currentMax")
//                                        maxBidsCRDT = maxBidsCRDT.add(addr, bid.price)
////                                        logger.info(s"Updated max value for CRDT: $maxBidsCRDT")
//                                    } else {
//                                        logger.debug(s"Received bid with price less than max value: $bid")
//                                    }
//                                case None =>
////                                    logger.info(s"No previous max value found. Initializing with: $bid")
//                                    maxBidsCRDT = maxBidsCRDT.add(addr, bid.price)
//                            }

                        case _ =>
                            logger.debug(s"Ignored non-bid event: $event")
                    }
                }

            case CHN_BROADCAST =>
                for (rec <- recs) {
                    crdtFromBinaryWithManifest(rec._2) match {
//                        case (Nexmark.BIDS_MANIFEST, delta) =>
//                            bidsCRDT = bidsCRDT.mergeDelta(delta.asInstanceOf[GCounter])

                        case (Nexmark.BIDS_MANIFEST, state: LWWRegister[Long]) =>
                            maxBidRegister = maxBidRegister.merge(state)
                            logger.info(s"Received broadcast and merged state: ${maxBidRegister.updatedBy} State: $state")

                        // Uncomment if needed for GSet merging
                        case (Nexmark.BIDS_MANIFEST, delta: GSet[(SelfUniqueAddress, Long)]) =>
                             maxBidsCRDT = maxBidsCRDT.mergeDelta(delta)
                             logger.debug(s"Received broadcast and merged delta: $maxBidsCRDT")

                        case _ =>
                            logger.warn(s"Ignored broadcast with unknown manifest or invalid data ${rec._2}")
                    }
                }

            case _ =>
                throw new RuntimeException(s"Unknown channel: $chn")
        }

    // Emit the latest CRDT value
    outputFunction(CHN_OUTPUT, Iterable.single((writeBinary(partition), writeBinary(bidsCRDT.value))))
    // outputFunction(CHN_OUTPUT, Iterable.single((writeBinary(partition), writeBinary(maxBidsCRDT.elements.map(_._2).maxOption.getOrElse(0L))))

    // Emit CRDT state (LWWRegister)
    outputFunction(CHN_BROADCAST, Iterable.single((writeBinary(partition), crdtToBinaryWithManifest(Nexmark.BIDS_MANIFEST, maxBidRegister))))

    // Emit CRDT delta values (if exists)
    if (bidsCRDT.delta.isDefined) {
        val delta = bidsCRDT.delta.get
        outputFunction(CHN_BROADCAST, Iterable.single((writeBinary(partition), crdtToBinaryWithManifest(Nexmark.BIDS_MANIFEST, delta))))
        bidsCRDT = bidsCRDT.resetDelta
    }

    // Emit CRDT delta values for maxBidsCRDT (if exists)
    // Uncomment this section if needed
    // if (maxBidsCRDT.delta.isDefined) {
    //     val delta = maxBidsCRDT.delta.get
    //     outputFunction(CHN_BROADCAST, Iterable.single((writeBinary(partition), crdtToBinaryWithManifest(Nexmark.BIDS_MANIFEST, delta))))
    //     maxBidsCRDT = maxBidsCRDT.resetDelta
    // }

    }

    override def snapshot(): Array[Byte] = {
        logger.info("Taking snapshot")
        crdtToBinaryWithManifest(Nexmark.BIDS_MANIFEST, bidsCRDT)
//        crdtToBinaryWithManifest(Nexmark.BIDS_MANIFEST, bidsCRDT)
        // If you also want to snapshot maxBidsCRDT, include it here:
        // crdtToBinaryWithManifest(Nexmark.BIDS_MANIFEST, maxBidsCRDT)
    }

    override def restore(snapshot: Array[Byte]): Unit = {
        logger.info("Restoring from snapshot")
//        bidsCRDT = crdtFromBinaryWithManifest(snapshot)._2.asInstanceOf[GCounter]
        // Restore maxBidsCRDT if needed
        // maxBidsCRDT = crdtFromBinaryWithManifest(snapshot)._2.asInstanceOf[GSet[(SelfUniqueAddress, Long)]]
    }
}