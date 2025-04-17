package holon.backend.messages

import upickle.default.{ReadWriter, macroRW}

sealed trait BroadcastMessage derives ReadWriter {
    def senderId: Int
}

// object BroadcastMessage {
//     implicit val rw: ReadWriter[BroadcastMessage] = macroRW
// }

case class CRDTUpdate(update: Array[Byte], senderId: Int, lag: Long) extends BroadcastMessage derives ReadWriter

// object CRDTUpdate {
//     implicit val rw: ReadWriter[CRDTUpdate] = macroRW
// }
