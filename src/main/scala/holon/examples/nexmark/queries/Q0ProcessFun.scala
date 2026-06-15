package holon.examples.nexmark.queries

import holon.utils.*
import holon.streaming.windowing.{WindowedRecordProcFunFullState, WindowedFullStateQueryFun}

// highest bid price
class Q0ProcessFun(partition: Int, crdts: List[WindowedRecordProcFunFullState[_, _]]) extends WindowedFullStateQueryFun(partition, crdts) {
  private val logger = Logger("Q0ProcessFun")
  Logger.setLevel("Q0ProcessFun", "INFO")
  logger.info("Starting Q0ProcessFun")

  override protected def processWindow(window: Long, states: List[Any]): String = {
    // states.head is LWWRegister[Array[Byte]]
    "--pass-through--"
  }
}