package holon

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeoutException

import scala.concurrent.duration.*
import scala.concurrent.Await
import scala.concurrent.Future

object Utils:

  //////////////////////////////////////////////////////////////////////////////
  // Thread utils and timeouts
  //////////////////////////////////////////////////////////////////////////////

  private case class Finished() extends Exception

  def CatchAll(f: => Unit): Unit =
    try f
    catch
      case Finished() =>
        System.exit(0)
      case e: Throwable =>
        print(e)
        e.printStackTrace()
        System.exit(1)

  def TerminateAfter(millis: Long)(f: => Unit): Unit =
    Await.result(
      Future(f)(using scala.concurrent.ExecutionContext.global),
      Duration(millis, MILLISECONDS)
    )

  /** Run for `millis` time then stop. All `Exception`s are caught, printed, and
    * will cause system exit.
    *
    * Throws `TimeoutException` if it takes longer than `millis` time.
    */
  def SafeRun(millis: Long)(f: => Unit): Unit =
    CatchAll(
      TerminateAfter(millis)({ f; throw Finished() })
    )

  def RunThread(f: => Unit): Thread =
    val thread = new Thread(() => f)
    thread.start()
    thread

  //////////////////////////////////////////////////////////////////////////////
  // Benchmarking utils
  //////////////////////////////////////////////////////////////////////////////

  class CountingCompletionWatcher(count: Int):
    private val atMost = 120.seconds
    private val countDownLatch = CountDownLatch(count)

    def complete(): Unit =
      countDownLatch.countDown()

    def waitForCompletion(): Unit =
      if !countDownLatch.await(atMost.length, atMost.unit) then throw TimeoutException()

  class BenchmarkTimer():
    var results = List[Long]()

    def run(f: => Unit) = {
      val tStart = System.nanoTime()
      f
      val tStop = System.nanoTime()
      results ::= (tStop - tStart)
    }

    private def toSeconds(nanos: Long) = nanos / 1_000_000_000.0
    // private def toMillis(nanos: Long)  = nanos / 1_000_000.0
    // private def toMicros(nanos: Long)  = nanos / 1_000.0
    // private def toNanos(nanos: Long)   = nanos

    private def average(results: List[Long]) = results.sum / results.length

    def statistics: String = "average(s) " + toSeconds(average(results))
