package holon.crdt.serialization

import org.apache.pekko.cluster.ddata.ReplicatedDelta
import upickle.legacy.{ReadWriter, readwriter}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

/**
 * General serialization for holon.crdt.CRDT deltas used across multiple queries.
 * This handles the Java serialization of ReplicatedDelta objects.
 */
object DeltaCRDTSerialization {

  // === custom ReadWriter for DeltaReplicatedData via Java serialization ===
  implicit val deltaRW: ReadWriter[ReplicatedDelta] =
    readwriter[Array[Byte]].bimap[ReplicatedDelta](
      delta => {
        val baos = new ByteArrayOutputStream()
        val oos = new ObjectOutputStream(baos)
        oos.writeObject(delta)
        oos.close()
        baos.toByteArray
      },
      bytes => {
        val bais = new ByteArrayInputStream(bytes)
        val ois = new ObjectInputStream(bais)
        val obj = ois.readObject().asInstanceOf[ReplicatedDelta]
        ois.close()
        obj
      }
    )
}
