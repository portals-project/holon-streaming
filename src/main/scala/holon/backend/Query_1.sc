import holon.example.CRDT
import org.apache.pekko.cluster.ddata.GCounter

import scala.util.Random

// Create unique addresses for the GCounter replicas.
val addr1 = CRDT.address(1)
val addr2 = CRDT.address(2)

// Set random seed for reproducibility.
Random.setSeed(42)

// Returns window given an event time.
def defineWindow(eventTime: Long): Long = {
  val windowDuration = 10_000L
  if eventTime % windowDuration == 0 then
    eventTime / windowDuration
  else
    (eventTime / windowDuration) + 1
}

// Define a map with the window as the key and the GCounter as the value.
val windowMap1 = scala.collection.mutable.Map.empty[Long, GCounter]
val windowMap2 = scala.collection.mutable.Map.empty[Long, GCounter]

val outputMap = scala.collection.mutable.Map.empty[Long, BigInt]

// Simulate stream.
for (i <- 1 to 10000) {
  val event = Random.nextLong(100_000)
  // Determine the window for the event.
  val window = defineWindow(event)

  if !windowMap1.contains(window) then
    windowMap1(window) = GCounter.empty
  if !windowMap2.contains(window) then
    windowMap2(window) = GCounter.empty

  if i % 2 == 0 then
    windowMap1(window) = windowMap1(window).increment(addr1, 1L)
  else
    windowMap2(window) = windowMap2(window).increment(addr2, 1L)
}

// Local Aggregate values
for ((k, v) <- windowMap1) {
  println(s"Window: $k, Value: ${v.value}")
}
println(s"----------------")
for ((k, v) <- windowMap2) {
  println(s"Window: $k, Value: ${v.value}")
}

// Simulate broadcasting and merging.
for ((k, v) <- windowMap1) {
  if windowMap2.contains(k) then
    windowMap2(k) = windowMap2(k).merge(v)
}
for ((k, v) <- windowMap2) {
  if windowMap1.contains(k) then
    windowMap1(k) = windowMap1(k).merge(v)
}

// Values should be the same
for ((k, v) <- windowMap1) {
  if windowMap2.contains(k) then
    assert(v.value == windowMap2(k).value)
    outputMap(k) = v.value
  //    println(s"Window: $k, Value: ${v.value}")
}
for ((k, v) <- windowMap2) {
  if windowMap1.contains(k) then
    assert(v.value == windowMap1(k).value)
  outputMap(k) = v.value
  //    println(s"Window: $k, Value: ${v.value}")
}

// Consume the output.
for ((k, v) <- outputMap) {
  println(s"Window: $k, Aggregate: $v")
}
println(s"----------------")