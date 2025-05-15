package holon.backend.checkpointmanager

import holon.*
import upickle.default.writeBinary
import holon.backend.messages.Checkpoint
import Config.CHN_CONTROL
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.collection.immutable.Map

class DecentralizedCheckpointManager(outputCollector: OutputCollector) extends CheckpointManager {

    private val directoryPath = "snapshots"

    // Stores (offsets, snapshots) for partitions of other nodes
    private val partitionSnapshots = scala.collection.mutable.Map[Int, (Long, String)]()

    private val logger = Logger.apply("DecentralizedCheckpointManager")
    private val outputLog = LoggerFactory.getLogger("com.holon.system.output")
    Logger.setLevel("DecentralizedCheckpointManager", "INFO")

    protected def savePartitionSnapshots(nodeId: Int, partitionSnapshots: scala.collection.mutable.Map[Int, (Long, String)]): Unit = {
        this.partitionSnapshots ++= partitionSnapshots
        partitionSnapshots.foreach((partitionId, snapshotContent) => {
            saveLocalFile(partitionSnapshotPath(partitionId), snapshotContentFormat(snapshotContent))
        })
    }

    /**
     * Save snapshots from other nodes to partitionSnapshots map and local storage.
     * If a snapshot already exists for a partition, only save the snapshot with the highest offset.
     */
    def saveSnapshotsFromOtherNodes(partitionSnapshots: Map[Int, (Long, String)]): Unit = {
        partitionSnapshots.foreach((partitionId, snapshotContent) => {
            val (offset, _) = snapshotContent
            if (this.partitionSnapshots.contains(partitionId)) {
                val (prevOffset, _) = this.partitionSnapshots(partitionId)
                // Save snapshot with highest offset
                if (offset > prevOffset) {
                    this.partitionSnapshots.put(partitionId, snapshotContent)
                    saveLocalFile(partitionSnapshotPath(partitionId), snapshotContentFormat(snapshotContent))
                }
            } else {
                this.partitionSnapshots.put(partitionId, snapshotContent)
                saveLocalFile(partitionSnapshotPath(partitionId), snapshotContentFormat(snapshotContent))
            }
        })
    }

    protected def saveNodeCheckpoint(nodeId: Int, nodeOffsets: String): Unit = {
        saveLocalFile(nodeSnapshotPath(nodeId), nodeOffsets)
    }

    /**
     * Recover checkpoint for all partitions and reset broadcast channel offset for node.
     */
    def recoverPartitionCheckpoints(nodeId: Int, partitionIds: List[Int],
        procFunctionPerPartition: scala.collection.mutable.Map[Int, ProcFun],
        consumerPerPartition: scala.collection.mutable.Map[Int, (Byte, LogConsumer)]): Unit = {

        // Restore snapshot for all partitions
        partitionIds.foreach(partitionId => {
            recoverCheckpointForPartition(partitionId, procFunctionPerPartition(partitionId), consumerPerPartition(partitionId)._2)
        })
    }

    def recoverNodeOffset(nodeId: Int, consumerPerPartition: scala.collection.mutable.Map[Int, (Byte, LogConsumer)]): Boolean = {
        val nodeFileExists = localFileExists(nodeSnapshotPath(nodeId))
        if (localFileExists(nodeSnapshotPath(nodeId))) {
            val fileContent = readLocalFile(nodeSnapshotPath(nodeId))
            logger.debug(s"Restoring broadcast & consumer channel offset for node $nodeId: $fileContent")
            setNodeConsumerOffsets(fileContent, consumerPerPartition)
        }
        nodeFileExists
    }

    /**
     * Recover checkpoint for a specific partition
     * If the partition snapshot exists in the cached partitionSnapshots map, restore it form there.
     * Otherwise, check if the snapshot exists in the local storage.
     */
    def recoverCheckpointForPartition(partitionId: Int, procFun: ProcFun, consumer: LogConsumer): Unit = {
        if (partitionSnapshots.contains(partitionId)) {
            val (offset, snapshotString) = partitionSnapshots(partitionId)
            restorePartitionSnapshot(partitionId, offset, snapshotString, procFun, consumer)
        } else if (localFileExists(partitionSnapshotPath(partitionId))) {
            val fileContent = readLocalFile(partitionSnapshotPath(partitionId))
            val Array(offset: String, snapshotString: String) = fileContent.split(":")
            restorePartitionSnapshot(partitionId, offset.toLong, snapshotString, procFun, consumer)
        }
    }

    private def partitionSnapshotPath(partitionId: Int): String = {
        s"./$directoryPath/partition$partitionId.txt"
    }

    private def snapshotContentFormat(snapshotContent: (Long, String)): String = {
        val (offset, snapshot) = snapshotContent
        s"$offset:$snapshot"
    }

    private def nodeSnapshotPath(nodeId: Int): String = {
        s"./$directoryPath/node$nodeId.txt"
    }

    private def saveLocalFile(filePath: String, content: String): Unit = {
        val path = Paths.get(filePath)
        Files.createDirectories(path.getParent)
        Files.write(path, content.getBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
    }

    private def readLocalFile(filePath: String): String = {
        Files.readString(Paths.get(filePath))
    }

    private def localFileExists(filePath: String): Boolean = {
        Files.exists(Paths.get(filePath))
    }

    def sendCheckpointMessage(nodeId: Int): Unit = {
        sendCheckpointMessage(nodeId, partitionSnapshots.toMap)
    }

    def sendCheckpointMessage(nodeId: Int, partitionSnapshots: Map[Int, (Long, String)]): Unit = {
        val message = Checkpoint(nodeId, partitionSnapshots)
        val serializedMessage = writeBinary(message)
        val records = List((writeBinary(0), serializedMessage))

        outputCollector.collect(CHN_CONTROL, records)
    }
}
