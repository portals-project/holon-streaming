package holon.backend

import holon.example.CRDT.address
import org.apache.pekko.cluster.ddata.{GCounter, LWWMap}
import upickle.default.{ReadWriter, macroRW, readwriter}

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

case class OwnershipEntry(nodeId: Int, version: Long)

object OwnershipEntry {
    implicit val rw: ReadWriter[OwnershipEntry] = macroRW
}

// TODO Check if the use of address is correct!
implicit val ownershipMapReadWriter: ReadWriter[LWWMap[Int, OwnershipEntry]] =
    readwriter[Map[Int, OwnershipEntry]].bimap(
        (m: LWWMap[Int, OwnershipEntry]) => m.entries,
        (map: Map[Int, OwnershipEntry]) => {
            val addr = address(-1) // Dummy address
            val emptyLWWMap = LWWMap.empty[Int, OwnershipEntry]
            map.foldLeft(emptyLWWMap) { case (lwwMap, (key, value)) =>
                lwwMap.put(addr, key, value, ownershipClock)
            }
        }
        )

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
