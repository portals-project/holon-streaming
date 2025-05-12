package holon.backend.mocks

import holon.*
import holon.Config

class MockLogConsumer(lagValue: Long) extends holon.LogConsumer {
    override def lag(): Long = lagValue

    override def poll(): LogConsumerRecords = {
        // Mock implementation
        Iterable.empty
    }
    override def seek(partition: Int, offset: Long): Unit = {
        // Mock implementation
    }
    override def offsets(): Iterable[(Int, Long)] = {
        // Mock implementation
        Iterable.empty
    }
    override def close(): Unit = {
        // Mock implementation
    }
}
