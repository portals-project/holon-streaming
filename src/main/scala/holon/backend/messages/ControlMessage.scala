package holon.backend.messages

import upickle.default.{ReadWriter, macroRW}

sealed trait ControlMessage derives ReadWriter {
    def senderId: Int
}

// object ControlMessage {
//     implicit val rw: ReadWriter[ControlMessage] = macroRW
// }

case class Heartbeat(senderId: Int) extends ControlMessage derives ReadWriter

// object Heartbeat {
//     implicit val rw: ReadWriter[Heartbeat] = macroRW
// }

case class Checkpoint(senderId: Int, partitionSnapshots: Map[Int, (Long, String)]) extends ControlMessage

object Checkpoint {
    implicit val rw: ReadWriter[Checkpoint] = macroRW
}

case class OwnershipState(ownershipMap: scala.collection.mutable.Map[Int, OwnershipEntry], senderId: Int) extends ControlMessage

object OwnershipState {
    implicit val rw: ReadWriter[OwnershipState] = macroRW
}

case class OwnershipEntry(nodeId: Int, version: Long)

object OwnershipEntry {
    implicit val rw: ReadWriter[OwnershipEntry] = macroRW
}

case class OwnershipStateRequest(senderId: Int) extends ControlMessage

object OwnershipStateRequest {
    implicit val rw: ReadWriter[OwnershipStateRequest] = macroRW
}

case class OwnershipTransferRequest(receiverId: Int, partitions: List[Int], senderId: Int) extends ControlMessage

object OwnershipTransferRequest {
    implicit val rw: ReadWriter[OwnershipTransferRequest] = macroRW
}

case class OwnershipTransferDenial(receiverId: Int, partitions: List[Int], senderId: Int) extends ControlMessage

object OwnershipTransferDenial {
    implicit val rw: ReadWriter[OwnershipTransferDenial] = macroRW
}
