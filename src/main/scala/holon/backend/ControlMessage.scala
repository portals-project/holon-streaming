package holon.backend

import upickle.default.{ReadWriter, macroRW}

sealed trait ControlMessage {
    def senderId: Int
}

object ControlMessage {
    implicit val rw: ReadWriter[ControlMessage] = macroRW
}

case class OwnershipRequest(receiverId: Int, partitions: List[Int], senderId: Int) extends ControlMessage

object OwnershipRequest {
    implicit val rw: ReadWriter[OwnershipRequest] = macroRW
}

case class OwnershipRequestAccepted(receiverId: Int, partitions: List[Int], senderId: Int) extends ControlMessage

object OwnershipRequestAccepted {
    implicit val rw: ReadWriter[OwnershipRequestAccepted] = macroRW
}
