package holon.backend

import holon.*
import holon.Config.CHN_OUTPUT
import holon.crdt.{CRDTWrapper, HighestBidLWWMapWrapper}
import holon.serialization.SerializationImplicits.lwwRegisterBytesRW
import org.apache.pekko.cluster.ddata.{LWWMap, LWWRegister}
import org.slf4j.LoggerFactory
import upickle.legacy.{readBinary, writeBinary}

// Highest Bid Price
class Q0ProcessFun(partition: Int, crdts: List[WindowedRecordProcFunFullState[_, _]]) extends WindowedFullStateQueryFun(partition, crdts) {
  private val logger = Logger("Q7ProcessFun")
  private val metricsLog = LoggerFactory.getLogger("com.holon.metrics")
  Logger.setLevel("Q7ProcessFun", "INFO")
  logger.info("Starting Q7ProcessFun")

  override protected def processWindow(window: Long, states: List[Any]): String = {
    // states.head is LWWRegister[Array[Byte]]
    "--pass-through--"
  }
}