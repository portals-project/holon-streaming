package holon.backend

import holon.Config.*
import holon.OutputCollector
import holon.messages.ControlMessage
import upickle.default.{readBinary, writeBinary}

class OutputMessageHandler(outputCollector: OutputCollector) {

    def sendControlMessage(message: ControlMessage): Unit = {
        val serializedMessage = writeBinary(message)
        val records = List((writeBinary(0), serializedMessage))
        outputCollector.collect(CHN_CONTROL, records)
    }

}
