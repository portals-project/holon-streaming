package holon.backend

import upickle.default.{ReadWriter, macroRW}

sealed trait BroadcastMessage {
    def senderId: Int
}

object BroadcastMessage {
    implicit val rw: ReadWriter[BroadcastMessage] = macroRW
}

case class CRDTUpdate(update: Array[Byte], senderId: Int) extends BroadcastMessage

object CRDTUpdate {
    implicit val rw: ReadWriter[CRDTUpdate] = macroRW
}

case class OwnershipRequest(receiverId: Int, partitions: List[Int], senderId: Int) extends BroadcastMessage

object OwnershipRequest {
    implicit val rw: ReadWriter[OwnershipRequest] = macroRW
}

case class OwnershipRequestAccepted(receiverId: Int, partitions: List[Int], senderId: Int) extends BroadcastMessage

object OwnershipRequestAccepted {
    implicit val rw: ReadWriter[OwnershipRequestAccepted] = macroRW
}
