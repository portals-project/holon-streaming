package holon

import upickle.default.*

// import sporks.*
// import sporks.given

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

// case class ProcFunRef(
//     packed: PackedSpork[ProcFun]
// ) derives ReadWriter

// case class JobRef(
//     consumersRef: List[ConsumerRef],
//     producersRef: List[ProducerRef],
//     procFunRef: ProcFunRef,
// ) derives ReadWriter

case class Job(
    consumers: List[ConsumerRef],
    producers: List[ProducerRef],
    partitions: List[Int]
)
