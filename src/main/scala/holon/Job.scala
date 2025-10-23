package holon

import upickle.default.*

case class ConsumerRef(
    chn: Byte,
    host: String,
    port: Int,
    topic: String,
    partitions: List[Int],
) derives ReadWriter

case class ProducerRef(
    chn: Byte,
    host: String,
    port: Int,
    topic: String,
) derives ReadWriter

case class Job(
    consumers: List[ConsumerRef],
    producers: List[ProducerRef],
    procFunFactory: ProcFunFactory,
    partitions: List[Int]
)
