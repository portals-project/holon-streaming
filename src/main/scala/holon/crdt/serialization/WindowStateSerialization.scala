package holon.crdt.serialization

import upickle.legacy.{ReadWriter, macroRW, readwriter}

import scala.collection.mutable

/**
 * General serialization for window state management used across multiple queries.
 * This handles WindowDelta, SnapShotState, and OutputState serialization.
 */
object WindowStateSerialization {

  // Generic implicit for mutable maps
  implicit def mutableMapReadWriter[K: ReadWriter, V: ReadWriter]: ReadWriter[mutable.Map[K, V]] =
    readwriter[Map[K, V]].bimap[mutable.Map[K, V]](
      _.toMap,
      m => mutable.Map.empty[K, V] ++ m
    )

  case class WindowDelta(
                          partition: Int,
                          queryId:  Int,
                          vectorClock: Array[Long],
                          window: Long,
                          deltas: List[Array[Byte]],
                          isFinal: Boolean
                        )
  object WindowDelta { given ReadWriter[WindowDelta] = macroRW }

  // not used anymore
  case class WindowState[T](
                             partition: Int,
                             queryId: Int,
                             vectorClock: Array[Long],
                             windowMap: mutable.Map[Long, (T, Boolean)]
                           )
  object WindowState { given windowStateRW[T: ReadWriter]: ReadWriter[WindowState[T]] = macroRW }

  case class SnapShotState[T](
                               emittedWindows: Long,
                               vectorClock: Array[Long],
                               windowMap: mutable.Map[Long, (T, Boolean)]
                             )
  object SnapShotState { given snapShotStateRW[T: ReadWriter]: ReadWriter[SnapShotState[T]] = macroRW }

  case class OutputState(
                          partition: Int,
                          window: Long,
                          value: String
                        )

  object OutputState {
    given ReadWriter[OutputState] = macroRW
  }
}
