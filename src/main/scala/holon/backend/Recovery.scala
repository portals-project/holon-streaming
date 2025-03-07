package holon.backend

import java.util.concurrent.ConcurrentLinkedQueue

import holon.*
import holon.Utils.*

class Recovery {
  private val consumers = scala.collection.mutable.Map.empty[Byte, LogConsumer]
  private val producers = scala.collection.mutable.Map.empty[Byte, LogProducer]
  private var procFun: ProcFun = null
  private val out = OutputCollectorImpl(producers)
  private val queue = new ConcurrentLinkedQueue[Job]()

  RunThread(this.run())

  def submitOrUpdate(job: Job): Unit = {
    this.queue.add(job)
  }

  private def setup(job: Job): Unit = {
    // setup consumers
    this.consumers.clear()
    job.consumers.foreach: ref =>
      val consumer = KafkaLogConsumer.fromRef(ref)
      this.consumers.put(ref.chn, consumer)

    // setup producers
    this.producers.clear()
    job.producers.foreach: ref =>
      val producer = KafkaLogProducer.fromRef(ref)
      this.producers.put(ref.chn, producer)

    // setup procFun
    this.procFun = job.procFun
  }

  private def run(): Unit = {
    var time = 0L
    while true do
      // check the job queue every 1_000 milliseconds
      val t = System.currentTimeMillis()
      if (t - time) > 1_000 then
        if this.procFun != null then
          val snap = this.procFun.snapshot()
          println(s"Snapshot: ${snap}")

        time = t
        checkJobQueue()
      runStep()
  }

  private inline def checkJobQueue(): Unit = {
    val job = this.queue.poll()
    if job != null then this.setup(job)
  }

  private inline def runStep(): Unit = {
    // 1. Poll, process each consumer
    for ((chn, consumer) <- consumers) {
      val records = consumer.poll()
      if !records.isEmpty then procFun.process(out, chn, records)
    }

    // 2. Flush all producers
    for ((chn, producer) <- producers) do producer.flush()
  }
}
