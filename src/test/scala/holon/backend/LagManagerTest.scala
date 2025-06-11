package holon.backend

import holon.*
import holon.Config
import org.scalatest.funsuite.AnyFunSuite
import holon.backend.mocks.MockLogConsumer

import scala.collection.mutable

class LagManagerTest extends AnyFunSuite {

    test("getNodeWithMaxLag: should return correct node") {
        assert(LagManager.getNodeWithMaxLag.isEmpty)

        LagManager.updateLag(0, 10)
        LagManager.updateLag(1, 20)
        LagManager.updateLag(2, 19)

        val maxLagNode = LagManager.getNodeWithMaxLag
        assert(maxLagNode.isDefined)
        assert(maxLagNode.get._1 == 1)
    }

    test("getPartitionWithMaxLag: should return partition with the largest lag") {
        val consumerPerPartition = scala.collection.mutable.Map.empty[Int, (Byte, LogConsumer)]
        consumerPerPartition.put(0, (0.toByte, new MockLogConsumer(10)))
        consumerPerPartition.put(1, (0.toByte, new MockLogConsumer(20)))
        consumerPerPartition.put(2, (0.toByte, new MockLogConsumer(15)))

        val result = LagManager.getPartitionWithMaxLag(consumerPerPartition)
        assert(result.isDefined)
        assert(result.get._1 == 1) // Partition 1 has the largest lag
        assert(result.get._2 == 20) // Lag value is 20
    }

    test("getPartitionWithMaxLag: should return None if all partitions are filtered out") {
        val consumerPerPartition = scala.collection.mutable.Map.empty[Int, (Byte, LogConsumer)]
        consumerPerPartition.put(Config.BROADCAST_PARTITION_ID, (0.toByte, new MockLogConsumer(10)))
        consumerPerPartition.put(Config.CONTROL_PARTITION_ID, (0.toByte, new MockLogConsumer(20)))

        val result = LagManager.getPartitionWithMaxLag(consumerPerPartition)
        assert(result.isEmpty)
    }

    test("getPartitionWithMaxLag: should return None if no consumers are provided") {
        val consumerPerPartition = mutable.Map.empty[Int, (Byte, LogConsumer)]

        val result = LagManager.getPartitionWithMaxLag(consumerPerPartition)
        assert(result.isEmpty)
    }

    test("calculateCurrentLagIfRequired: should calculate current lag when interval has passed") {
        val consumerPerPartition = scala.collection.mutable.Map.empty[Int, (Byte, LogConsumer)]
        consumerPerPartition.put(0, (0.toByte, new MockLogConsumer(10)))
        consumerPerPartition.put(1, (0.toByte, new MockLogConsumer(20)))
        consumerPerPartition.put(2, (0.toByte, new MockLogConsumer(15)))

        LagManager.lagCalculationTime = System.currentTimeMillis() - 2000 // Simulate interval passed
        LagManager.calculateCurrentLagIfRequired(consumerPerPartition)

        assert(LagManager.getCurrentLag == 45) // 10 + 20 + 15
    }

    test("calculateCurrentLagIfRequired: should handle empty consumer map") {
        val consumerPerPartition = mutable.Map.empty[Int, (Byte, LogConsumer)]

        LagManager.lagCalculationTime = System.currentTimeMillis() - 2000 // Simulate interval passed
        LagManager.calculateCurrentLagIfRequired(consumerPerPartition)

        assert(LagManager.getCurrentLag == 0) // No lag to calculate
    }
}
