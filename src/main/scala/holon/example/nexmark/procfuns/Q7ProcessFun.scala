package holon.example.nexmark.procfuns

import holon.*
import holon.Config.CHN_OUTPUT
import holon.backend.{WindowedQueryFun, WindowedRecordProcFun}
import holon.crdt.{CRDTWrapper, HighestBidLWWRegisterWrapper}
import holon.serialization.SerializationImplicits.lwwRegisterBytesRW
import org.apache.pekko.cluster.ddata.LWWRegister
import org.slf4j.LoggerFactory
import upickle.legacy.{readBinary, writeBinary}

// Highest Bid Price
class Q7ProcessFun(partition: Int, crdts: List[WindowedRecordProcFun[_, _]]) extends WindowedQueryFun(partition, crdts) {
  private val logger = Logger("Q7ProcessFun")
  private val metricsLog = LoggerFactory.getLogger("com.holon.metrics")
  Logger.setLevel("Q7ProcessFun", "INFO")
  logger.info("Starting Q7ProcessFun")

  override protected def processWindow(window: Long, states: List[Any]): String = {
    // states.head is LWWRegister[Array[Byte]]
    HighestBidLWWRegisterWrapper.value(states.head.asInstanceOf[LWWRegister[Array[Byte]]])
  }
}