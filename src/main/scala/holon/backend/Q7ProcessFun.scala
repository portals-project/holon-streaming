package holon.backend

import holon.*
import holon.Config.CHN_OUTPUT
import holon.crdt.{CRDTWrapper, HighestBidLWWMapWrapper}
import holon.serialization.SerializationImplicits.lwwRegisterBytesRW
import org.apache.pekko.cluster.ddata.{LWWMap, LWWRegister}
import org.slf4j.LoggerFactory
import upickle.legacy.{readBinary, writeBinary}

// highest bid price
class Q7ProcessFun(partition: Int, crdts: List[WindowedRecordProcFun[_, _]]) extends WindowedQueryFun(partition, crdts) {
  private val logger = Logger("Q7ProcessFun")
  private val metricsLog = LoggerFactory.getLogger("com.holon.metrics")
  Logger.setLevel("Q7ProcessFun", "INFO")
  logger.info("Starting Q7ProcessFun")

  override protected def processWindow(window: Long, states: List[Any]): String = {
    HighestBidLWWMapWrapper.value(states.head.asInstanceOf[LWWMap[String, Array[Byte]]])
  }
}