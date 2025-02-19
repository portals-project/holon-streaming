package holon.backend

import org.apache.pekko.cluster.ddata.{LWWRegister, SelfUniqueAddress}
import upickle.default._
import holon._
import holon.example.nexmark.Config._
import holon.example.Nexmark
import holon.example.CRDT._

/**
 * This ProcFun implementation applies a tumbling window to incoming bid events.
 * Each window (of windowDuration milliseconds) maintains its own LWWRegister,
 * which is merged into a global register when the window expires.
 */
class WindowedRecordProcFun(partition: Int) extends ProcFun {
  private val addr: SelfUniqueAddress = address(partition)
  private val logger = Logger.apply("WindowedRecordProcFun")
  Logger.setLevel("WindowedRecordProcFun", "INFO")

  // Window configuration: for example, a 2-second tumbling window.
  private val windowDuration: Long = 2000L  // duration in milliseconds
  private var currentWindowStart: Long = System.currentTimeMillis()
  // Register for the current window
  private var currentWindowRegister: LWWRegister[Long] = LWWRegister(addr, 0L)
  // Global register aggregates the results from all closed windows
  private var globalMaxRegister: LWWRegister[Long] = LWWRegister(addr, 0L)
  // (for debug) Keep track of how many windows have been processed
  private var windowCount: Int = 0

  override def process(
                        outputFunction: (Byte, LogProducerRecords) => Unit,
                        chn: Byte,
                        recs: LogConsumerRecords
                      ): Unit = {
    val now = System.currentTimeMillis()
    // Check if the current window has expired.
    if (now >= currentWindowStart + windowDuration) {
      // Merge the finished window into the global register.
      globalMaxRegister = globalMaxRegister.merge(currentWindowRegister)
      logger.info(
        s"Window ${windowCount} closed. Merged window max ${currentWindowRegister.value} into global max ${globalMaxRegister.value}"
      )

      // Optionally, emit the closed window result before resetting.
      outputFunction(CHN_OUTPUT, Iterable.single((writeBinary(partition), writeBinary(globalMaxRegister.value))))

      // Reset the window: start a new window with an empty register.
      currentWindowStart = now
      currentWindowRegister = LWWRegister(addr, 0L)
      // Increment the window count.
      windowCount += 1
    }

    chn match {
      case CHN_NEXMARK =>
        // Process incoming bids.
        for (rec <- recs) {
          val event = Nexmark.deserialize(rec._2).event
          event match {
            case bid: Nexmark.Events.Bid =>
              // If this bid’s price is higher than the current window’s value, update it.
              if (bid.price > currentWindowRegister.value) {
                currentWindowRegister = currentWindowRegister.withValue(addr, bid.price)
                logger.debug(s"Updated current window max to ${bid.price}")
              }
            case _ =>
              logger.debug(s"Ignored non-bid event: $event")
          }
        }

      case CHN_BROADCAST =>
        // Merge state received from other nodes.
        for (rec <- recs) {
          crdtFromBinaryWithManifest(rec._2) match {
            case (Nexmark.BIDS_MANIFEST, state: LWWRegister[Long]) =>
              // Merge the broadcast state into both the global and current window registers.
              globalMaxRegister = globalMaxRegister.merge(state)
              currentWindowRegister = currentWindowRegister.merge(state)
              logger.debug(s"Merged broadcast state with value ${state.value}")
            case _ =>
              logger.debug(s"Ignored unknown broadcast event with data: ${rec._2}")
          }
        }

      case _ =>
        throw new RuntimeException(s"Unknown channel: $chn")
    }

    // Emit the current global max value.
    outputFunction(CHN_OUTPUT, Iterable.single((writeBinary(partition), writeBinary(globalMaxRegister.value))))
    // Broadcast the current window state.
    outputFunction(CHN_BROADCAST, Iterable.single((writeBinary(partition), crdtToBinaryWithManifest(Nexmark.BIDS_MANIFEST, currentWindowRegister))))
  }

  override def snapshot(): Array[Byte] = {
    // Snapshot the global state
    crdtToBinaryWithManifest(Nexmark.BIDS_MANIFEST, globalMaxRegister)
  }

  override def restore(snapshot: Array[Byte]): Unit = {
    globalMaxRegister = crdtFromBinaryWithManifest(snapshot)._2.asInstanceOf[LWWRegister[Long]]
  }
}
