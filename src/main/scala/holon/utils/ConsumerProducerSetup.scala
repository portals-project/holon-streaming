package holon.utils

import holon.core.{Config, ConsumerRef, ProducerRef}
import holon.core.Config.*
import holon.utils.*

object ConsumerProducerSetup {

    private val logger = Logger.apply("ConsumerProducerSetup")
    Logger.setLevel("ConsumerProducerSetup", "INFO")

    /**
     * Setup producers from producer references.
     */
    def setupProducers(producerRefs: List[ProducerRef], producers: scala.collection.mutable.Map[Byte, LogProducer]): Unit = {
        producerRefs.foreach { ref =>
            val producer = holon.streaming.messaging.KafkaLogProducer.fromRef(ref)
            producers.put(ref.chn, producer)
        }
    }

    /**
     * Setup internal consumers for CONTROL and BROADCAST channels.
     * @return Tuple of control and broadcast consumers.
     */
    def setupInternalConsumers(consumerRefs: List[ConsumerRef], consumerPerPartition: scala.collection.mutable.Map[Int, (Byte, LogConsumer)]): (holon.streaming.messaging.KafkaLogConsumer, holon.streaming.messaging.KafkaLogConsumer) = {
        val controlConsumer = holon.streaming.messaging.KafkaLogConsumer.fromRef(consumerRefs.find(_.chn == CHN_CONTROL).get)
        consumerPerPartition.put(CONTROL_PARTITION_ID, (CHN_CONTROL, controlConsumer))
        val broadcastConsumer = holon.streaming.messaging.KafkaLogConsumer.fromRef(consumerRefs.find(_.chn == CHN_BROADCAST).get)
        consumerPerPartition.put(BROADCAST_PARTITION_ID, (CHN_BROADCAST, broadcastConsumer))
        (controlConsumer, broadcastConsumer)
    }

    /**
     * Setup consumers for each partition owned by the node.
     */
    def setupPartitionConsumers(consumerRefs: List[ConsumerRef], partitionsOwned: List[Int],
        consumerPerPartition: scala.collection.mutable.Map[Int, (Byte, LogConsumer)]): Unit = {

        consumerRefs.foreach { ref =>
            // Don't setup CONTROL or BROADCAST channel consumer here. It is setup separately.
            // Also, don't setup Input consumer if partition is not owned by this node.
            if (ref.chn == CHN_INPUT && partitionsOwned.contains(ref.partitions.head)) {
                val consumer = holon.streaming.messaging.KafkaLogConsumer.fromRef(ref)
                logger.debug(s"Setting up consumer: $consumer for partition ${consumer.partition}")
                consumerPerPartition.put(consumer.partition, (ref.chn, consumer))
            }
        }

        // Setup consumers for other owned partitions
        for (partitionId <- partitionsOwned) {
            if (!consumerPerPartition.contains(partitionId)) {
                addNewInputConsumer(partitionId, consumerPerPartition)
            }
        }
    }

    /**
     * Add new Input consumer for the partition.
     * @return The new consumer.
     */
    def addNewInputConsumer(partitionId: Int, consumerPerPartition: scala.collection.mutable.Map[Int, (Byte, LogConsumer)]): LogConsumer = {
        val inputConsumer = holon.streaming.messaging.KafkaLogConsumer.fromRef(ConsumerRef(
            chn = CHN_INPUT,
            host = KAFKA_HOST,
            port = KAFKA_PORT,
            topic = KAFKA_TOPIC_INPUT,
            partitions = List(partitionId),
            ))
        logger.debug(s"Adding new consumer for partition $partitionId: $inputConsumer")
        consumerPerPartition.put(partitionId, (CHN_INPUT, inputConsumer))
        inputConsumer
    }

}
