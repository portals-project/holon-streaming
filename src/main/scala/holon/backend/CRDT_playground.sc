import holon.example.CRDT.{crdtFromBinaryWithManifest, crdtToBinaryWithManifest}
import holon.example.Nexmark.Events.Event
import holon.example.{CRDT, Nexmark}
import org.apache.pekko.cluster.ddata.GCounter
import upickle.default.writeBinary

import scala.util.Random
println("--- Welcome to the CRDT playground! ---")
// Create unqiue addresses for the GCounter replicas.
val addr1 = CRDT.address(1)
val addr2 = CRDT.address(2)

// Set random seed for reproducibility.
Random.setSeed(42)

// Window duration in milliseconds.
val windowDuration = 10_000L

val eventMap = scala.collection.mutable.Map.empty[Long, Long]

// Returns window given an event time.
def defineWindow(eventTime: Long): Long = {
  if eventTime % windowDuration == 0 then
    eventTime / windowDuration
  else
    (eventTime / windowDuration) + 1
}

for i <- 1 to 10_000 do
  val event = Random.nextLong(50_000L)
  val window = defineWindow(event)
  if !eventMap.contains(window) then
    eventMap(window) = 1
  else
    eventMap(window) = eventMap(window) + 1
    
println(s"Number of events per window: $eventMap")

var testCRDT = GCounter.empty
testCRDT = testCRDT.increment(addr1, 1)
testCRDT = testCRDT.increment(addr1, 2)

println(s"Initial CRDT value: ${testCRDT.getValue}")

val binary = crdtToBinaryWithManifest(Nexmark.BIDS_MANIFEST, testCRDT)

testCRDT = testCRDT.increment(addr1, 2)

testCRDT = crdtFromBinaryWithManifest(binary)._2.asInstanceOf[GCounter]

println(s"Deserialized CRDT value: ${testCRDT.value}")

// Define a map with the window as the key and the GCounter as the value.
val windowMap1 = scala.collection.mutable.Map.empty[Long, GCounter]
val windowMap2 = scala.collection.mutable.Map.empty[Long, GCounter]

val outputMap = scala.collection.mutable.Map.empty[Long, BigInt]

// Simulate stream.
for (i <- 1 to 10000) {
  val event = Random.nextLong(10000)
//  println(s"event: $event")
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

println(s"----------------")
// Consume the output.
for ((k, v) <- outputMap) {
  println(s"Window: $k, Value: $v")
}

println("--- End of the CRDT playground! ---")










