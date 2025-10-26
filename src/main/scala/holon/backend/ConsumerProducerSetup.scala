package holon.backend

import holon.Config.*
import holon.*
import holon.backend.kafka.{KafkaLogConsumer, KafkaLogProducer}

import scala.collection.*

object ConsumerProducerSetup {

    private val logger = Logger.apply("ConsumerProducerSetup")
    Logger.setLevel("ConsumerProducerSetup", "INFO")

    /**
     * Setup producers from producer references.
     */
    def setupProducers(producerRefs: List[ProducerRef], producers: mutable.Map[Byte, LogProducer]): Unit = {
        producers.clear()
        producerRefs.foreach { ref =>
            val producer = KafkaLogProducer.fromRef(ref)
            producers.put(ref.chn, producer)
        }
    }

    /**
     * Setup internal consumers for CONTROL and BROADCAST channels.
     * @return Tuple of control and broadcast consumers.
     */
    def setupInternalConsumers(consumerRefs: List[ConsumerRef], consumerPerPartition: mutable.Map[Int, (Byte, LogConsumer)]): (KafkaLogConsumer, KafkaLogConsumer) = {
        val controlConsumer = KafkaLogConsumer.fromRef(consumerRefs.find(_.chn == CHN_CONTROL).get)
        consumerPerPartition.put(CONTROL_PARTITION_ID, (CHN_CONTROL, controlConsumer))
        val broadcastConsumer = KafkaLogConsumer.fromRef(consumerRefs.find(_.chn == CHN_BROADCAST).get)
        consumerPerPartition.put(BROADCAST_PARTITION_ID, (CHN_BROADCAST, broadcastConsumer))
        (controlConsumer, broadcastConsumer)
    }

    /**
     * Setup consumers for each partition owned by the node.
     */
    def setupPartitionConsumers(consumerRefs: List[ConsumerRef], partitionsOwned: List[Int],
        consumerPerPartition: mutable.Map[Int, (Byte, LogConsumer)]): Unit = {

        consumerRefs.foreach { ref =>
            // Don't setup CONTROL or BROADCAST channel consumer here. It is setup separately.
            // Also, don't setup Input consumer if partition is not owned by this node.
            if (ref.chn == CHN_INPUT && partitionsOwned.contains(ref.partitions.head)) {
                val consumer = KafkaLogConsumer.fromRef(ref)
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
    def addNewInputConsumer(partitionId: Int, consumerPerPartition: mutable.Map[Int, (Byte, LogConsumer)]): LogConsumer = {
        val inputConsumer = KafkaLogConsumer.fromRef(ConsumerRef(
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
