package holon.backend

import holon.*
import holon.Config.CHN_OUTPUT
import holon.crdt.{CRDTWrapper, HighestBidLWWMapWrapper}
import holon.serialization.NexmarkQ7Serialization.lwwRegisterBytesRW
import org.apache.pekko.cluster.ddata.{LWWMap, LWWRegister}
import org.slf4j.LoggerFactory
import upickle.legacy.{readBinary, writeBinary}

// highest bid price
class Q0ProcessFun(partition: Int, crdts: List[WindowedRecordProcFunFullState[_, _]]) extends WindowedFullStateQueryFun(partition, crdts) {
  private val logger = Logger("Q0ProcessFun")
  private val metricsLog = LoggerFactory.getLogger("com.holon.metrics")
  Logger.setLevel("Q0ProcessFun", "INFO")
  logger.info("Starting Q0ProcessFun")

  override protected def processWindow(window: Long, states: List[Any]): String = {
    // states.head is LWWRegister[Array[Byte]]
    "--pass-through--"
  }
}