package holon.backend

import holon.Logger
import holon.crdt.HighestBidLWWMapWrapper
import org.apache.pekko.cluster.ddata.LWWMap
import org.slf4j.LoggerFactory

class TQ2ProcessFun (partition: Int, crdts: List[WindowedRecordProcFun[_, _]]) extends WindowedQueryFun(partition, crdts) {
  private val logger = Logger("TQ2ProcessFun")
  private val metricsLog = LoggerFactory.getLogger("com.holon.metrics")
  Logger.setLevel("TQ2ProcessFun", "INFO")
  logger.info("Starting TQ2ProcessFun")

  override protected def processWindow(window: Long, states: List[Any]): String = {
    // states.head is LWWRegister[Array[Byte]]
    HighestBidLWWMapWrapper.value(states.head.asInstanceOf[LWWMap[String, Array[Byte]]])
  }
}
