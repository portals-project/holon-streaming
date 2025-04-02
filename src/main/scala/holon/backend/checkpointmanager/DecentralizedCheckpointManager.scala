package holon.backend.checkpointmanager

import holon.backend.checkpointmanager.CheckpointManager
import holon.example.nexmark.Config.*
import holon.*

abstract class DecentralizedCheckpointManager extends CheckpointManager {

    // Stores snapshots for partitions of other nodes
    private var partitionSnapshots = scala.collection.mutable.Map[Int, String]()

    private val logger = Logger.apply("DecentralizedCheckpointManager")
    Logger.setLevel("DecentralizedCheckpointManager", "INFO")



}
