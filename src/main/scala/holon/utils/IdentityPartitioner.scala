package holon.utils

import upickle.default.*

import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.*

class IdentityPartitioner extends Partitioner {
  import java.util.Map

  override def partition(
      topic: String,
      key: Any,
      keyBytes: Array[Byte],
      value: Any,
      valueBytes: Array[Byte],
      cluster: Cluster
  ): Int = {
    readBinary(keyBytes)
  }

  override def close(): Unit = ()

  override def configure(configs: Map[String, _]): Unit = ()
}

