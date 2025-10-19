# Serialization Organization

These files contain serialization logic used across multiple queries:

### `DeltaCRDTSerialization.scala`
- Contains serialization for CRDT deltas (`ReplicatedDelta`)
- Used by all queries that need delta-based CRDT operations

### `WindowStateSerialization.scala`
- Contains serialization for window state management
- Includes `WindowDelta`, `SnapShotState`, `OutputState`, and `WindowState`
- Provides generic `mutableMapReadWriter` for mutable maps

### `AuctionPopularitySerialization.scala`
- Contains serialization for auction popularity tracking using `GCounter`
- Used by queries that track auction popularity metrics

## Query-Specific Serializations

These files contain serialization logic specific to individual Nexmark queries:

### `NexmarkQ0Serialization.scala`
- Serialization for Nexmark Query 0 (pass-through query)
- Uses `GCounter` serialization with `gcounterRW`
- Window map serialization for Q0

### `NexmarkQ4Serialization.scala`
- Serialization for Nexmark Query 4 (auction to highest bid and category mapping)
- Uses `GSet[(Long, Long)]` serialization with `rwTuple`
- Includes `ByteCodec` trait for custom byte serialization
- Supports `LWWMap`, `ORMap`, and `GSet` CRDTs

### `NexmarkQ7Serialization.scala`
- Serialization for Nexmark Query 7 (highest bid tracking)
- Uses `LWWMap[String, Array[Byte]]` serialization with `lwwMapBytesRW`
- Includes `LWWRegister` and `ORSet` serialization support

## Usage

Each query factory imports the appropriate serialization:

```scala
// Q0Factory
import holon.serialization.NexmarkQ0Serialization.gcounterRW

// Q4Factory
import holon.serialization.NexmarkQ4Serialization.rwTuple

// Q7Factory
import holon.serialization.NexmarkQ7Serialization.lwwMapBytesRW
```

The `WindowedRecordProcFun.scala` imports general serializations:

```scala
import holon.serialization.{DeltaCRDTSerialization, WindowStateSerialization}
import DeltaCRDTSerialization.deltaRW
import WindowStateSerialization.{WindowDelta, WindowState, SnapShotState, OutputState, mutableMapReadWriter}
```