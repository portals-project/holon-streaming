package holon.backend

import holon.{LogConsumerRecords, Logger, OutputCollector, ProcFun}
import holon.example.CRDT.{address, crdtFromBinaryWithManifest, crdtToBinaryWithManifest}
import holon.example.Nexmark
import holon.example.nexmark.Config.{CHN_BROADCAST, CHN_NEXMARK, CHN_OUTPUT}
import org.apache.pekko.cluster.ddata.GCounter
import upickle.default.writeBinary

class QueryProcFun(partition: Int) extends ProcFun {
  //  TODO: Implement different CRDTs for micro benchmarks
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
}
