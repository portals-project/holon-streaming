package holon.backend.kafka

object KafkaSerdes:
  final val KAFKA_SER =
    org.apache.kafka.common.serialization.Serdes
      .ByteArray()
      .serializer()
      .getClass
      .getName

  final val KAFKA_DES =
    org.apache.kafka.common.serialization.Serdes
      .ByteArray()
      .deserializer()
      .getClass
      .getName
