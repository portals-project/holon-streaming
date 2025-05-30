package holon.backend

import holon.*
import holon.Config.CONTROL_PARTITION_ID
import holon.{LogConsumer, ProcFun}
import holon.ProcFunFactory
import holon.backend.ConsumerProducerSetup
import holon.backend.checkpointmanager.CheckpointManager

import scala.collection.mutable

class ControlState(procFunFactory: ProcFunFactory, checkpointManager: CheckpointManager) {

    val consumerPerPartition = mutable.Map.empty[Int, (Byte, LogConsumer)]
    var procFunctionPerPartition = mutable.Map.empty[Int, ProcFun]

    private val logger = Logger.apply("ControlState")
    Logger.setLevel("ControlState", "INFO")

    def getControlConsumer(): LogConsumer = {
        val (_, controlConsumer) = this.consumerPerPartition.getOrElse(CONTROL_PARTITION_ID, (0, null))
        controlConsumer
    }

    /**
     * Setup processing functions for each partition owned.
     */
    def setupProcFunctions(partitionsOwned: List[Int]): Unit = {
        this.procFunctionPerPartition = mutable.Map(partitionsOwned.map { partition =>
            partition -> procFunFactory.create(partition)
        }: _*)
    }

    /**
     * Integrate new partitions into the system. Create processing function, consumer & restore snapshot.
     */
    def integrateNewPartitions(partitions: List[Int]): Unit = {
        for (partitionId <- partitions) {
            val procFun = procFunFactory.create(partitionId)
            procFunctionPerPartition += partitionId -> procFun
            val consumer = ConsumerProducerSetup.addNewInputConsumer(partitionId, consumerPerPartition)

            checkpointManager.recoverCheckpointForPartition(partitionId, procFun, consumer)
        }
    }

    def dropPartition(partitionId: Int): Unit = {
        val (_chn, consumer) = this.consumerPerPartition(partitionId)
        if (consumer == null) {
            logger.warn(s"Consumer for partition $partitionId is null. Cannot remove consumer and processing function")
            return
        }
        consumer.close()
        if this.procFunctionPerPartition.contains(partitionId) then
            this.procFunctionPerPartition.remove(partitionId)

        if this.consumerPerPartition.contains(partitionId) then
            this.consumerPerPartition.remove(partitionId)
    }

}
