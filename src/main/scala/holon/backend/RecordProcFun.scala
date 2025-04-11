package holon.backend

import upickle.default.{readBinary, writeBinary}
import holon.*
import Config.*
import holon.example.{CRDT, Nexmark}
import holon.example.CRDT.address
import holon.backend.DistributedCounter.Counter
import org.apache.pekko.cluster.ddata.GCounter
import upickle.legacy.{ReadWriter, readwriter}

/**
 * A simplified message processing class that counts bid events using an immutable CRDT counter.
 * It uses RecordState to store the counter (which is a SimpleCounter.Counter) and emits the state periodically.
 */
class RecordProcFun(partition: Int) extends ProcFun {
    // The processing state, using RecordState.
    private var state: RecordState = RecordState()
    // Use the partition as the node identifier.
    private val addr = address(partition)

    // Timing and message count parameters.
    private val emitInterval = 3_000L
    private var lastEmitTime = System.currentTimeMillis()
    private var messageCount = 30

    private val logger = Logger("RecordProcFunction")
    Logger.setLevel("RecordProcFunction", "INFO")
    logger.info("Starting RecordProcFunction")

    override def process(
                          outputFunction: (Int, Byte, LogProducerRecords) => Unit,
                          chn: Byte,
                          recs: LogConsumerRecords
                        ): Unit = {
        // Periodically emit the CRDT value to the output channel.
        if (System.currentTimeMillis() - lastEmitTime > emitInterval) {
            logger.debug("Emitting CRDT state to output channel")
            outputFunction(
                partition,
                CHN_OUTPUT,
                Iterable.single((writeBinary(0), writeBinary(DistributedCounter.valueCounter(state.counter))))
            )
            lastEmitTime = System.currentTimeMillis()
        }

        chn match {
            case CHN_INPUT =>
                // Process bid events from the Nexmark stream.
                for (rec <- recs) {
                    val event = Nexmark.deserialize(rec._2).event
                    event match {
                        case bid: Nexmark.Events.Bid =>
                            state = state.copy(counter = DistributedCounter.updateCounter(state.counter, addr, 1L))
                        case _ =>
                        // Ignore non-bid events.
                    }
                }
            case CHN_BROADCAST =>
                // Merge state received from other nodes.
                for (rec <- recs) {
                    val receivedCounter = readBinary[DistributedCounter.Counter](rec._2)
                    state = state.copy(counter = DistributedCounter.mergeCounter(state.counter, receivedCounter))
                }
            case _ =>
                throw new RuntimeException(s"Unknown channel: $chn")
        }

        // Every 30 messages, broadcast the current CRDT state.
        messageCount += 1
        if (messageCount >= 10) {
            logger.debug(s"Partition $partition emitting CRDT state to broadcast channel")
            outputFunction(
                partition,
                CHN_BROADCAST,
                Iterable.single((writeBinary(0), writeBinary(DistributedCounter.valueCounter(state.counter))))
            )
            messageCount = 0
        }
    }

    override def snapshot(): Array[Byte] = {
        logger.debug("Taking snapshot")
        writeBinary(state)
    }

    override def restore(snapshot: Array[Byte]): Unit = {
        logger.debug("Restoring from snapshot")
        state = readBinary[RecordState](snapshot)
        logger.info(s"Partition $partition restored CRDT: ${state.counter}")
    }

    // For RecordProcFun, windowing is not used.
    override def defineWindow(eventTime: Long): Long = 0L
}
