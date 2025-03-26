package holon.backend

import org.apache.pekko.cluster.ddata.LWWMap
import upickle.default.{ReadWriter, macroRW}

sealed trait ControlMessage {
    def senderId: Int
}

object ControlMessage {
    implicit val rw: ReadWriter[ControlMessage] = macroRW
}

case class OwnershipState(ownershipMap: LWWMap[Int, OwnershipEntry], senderId: Int) extends ControlMessage

object OwnershipState {
    implicit val rw: ReadWriter[OwnershipState] = macroRW
}

case class OwnershipStateRequest(senderId: Int) extends ControlMessage

object OwnershipStateRequest {
    implicit val rw: ReadWriter[OwnershipStateRequest] = macroRW
}

case class OwnershipTransferRequest(receiverId: Int, partitions: List[Int], senderId: Int) extends ControlMessage

object OwnershipTransferRequest {
    implicit val rw: ReadWriter[OwnershipTransferRequest] = macroRW
}

case class OwnershipTransferConfirmation(receiverId: Int, partitions: List[Int], senderId: Int) extends ControlMessage

object OwnershipTransferConfirmation {
    implicit val rw: ReadWriter[OwnershipTransferConfirmation] = macroRW
}

case class OwnershipTransferDenial(receiverId: Int, partitions: List[Int], senderId: Int) extends ControlMessage

object OwnershipTransferDenial {
    implicit val rw: ReadWriter[OwnershipTransferDenial] = macroRW
}
