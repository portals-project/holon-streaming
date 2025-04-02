package holon.backend

import upickle.default.{ReadWriter, macroRW}

sealed trait BroadcastMessage {
    def senderId: Int
}

object BroadcastMessage {
    implicit val rw: ReadWriter[BroadcastMessage] = macroRW
}

case class CRDTUpdate(update: Array[Byte], senderId: Int, lag: Long) extends BroadcastMessage

object CRDTUpdate {
    implicit val rw: ReadWriter[CRDTUpdate] = macroRW
}

case class NodeCheckpoint(senderId: Int, partitionSnapshots: Map[Int, String]) extends BroadcastMessage

object NodeCheckpoint {
    implicit val rw: ReadWriter[NodeCheckpoint] = macroRW
}
