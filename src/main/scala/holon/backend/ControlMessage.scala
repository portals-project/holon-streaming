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

case class OwnershipRequestConfirmation(receiverId: Int, partitions: List[Int], senderId: Int) extends ControlMessage

object OwnershipRequestConfirmation {
    implicit val rw: ReadWriter[OwnershipRequestConfirmation] = macroRW
}

case class OwnershipRequestDenial(receiverId: Int, partitions: List[Int], senderId: Int) extends ControlMessage

object OwnershipRequestDenial {
    implicit val rw: ReadWriter[OwnershipRequestDenial] = macroRW
}
