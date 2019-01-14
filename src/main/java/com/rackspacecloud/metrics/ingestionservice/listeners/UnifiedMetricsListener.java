package com.rackspacecloud.metrics.ingestionservice.listeners;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Instant;
import java.util.Map;

public class UnifiedMetricsListener implements ConsumerSeekAware {

    protected long batchProcessedCount = 0;

    protected Counter counter;


    // At the end of every 1000 messages, log this information
    protected static final int MESSAGE_PROCESS_REPORT_COUNT = 1000;

    private static final Logger LOGGER = LoggerFactory.getLogger(UnifiedMetricsListener.class);

    protected MeterRegistry registry;

    @Autowired
    public UnifiedMetricsListener(MeterRegistry registry) {
        this.registry = registry;
        this.counter = this.registry.counter("batch.processed");
    }

    @Override
    public void registerSeekCallback(ConsumerSeekCallback consumerSeekCallback) {
        LOGGER.info("Registering seekCallback at [{}]", Instant.now());
    }

    protected boolean processPostInfluxDbIngestion(
            final String recordsString, final int partitionId, final long offset,
            final Acknowledgment ack, final boolean isInfluxdbIngestionSuccessful) {

        boolean isSuccessful = isInfluxdbIngestionSuccessful;

        if (isInfluxdbIngestionSuccessful) {
            ack.acknowledge();
            LOGGER.debug("Successfully processed partitionId:{}, offset:{} at {}",
                    partitionId, offset, Instant.now());

            if (batchProcessedCount % MESSAGE_PROCESS_REPORT_COUNT == 0) {
                LOGGER.info("Processed {} batches.", batchProcessedCount);
            }
        } else {
            LOGGER.error("FAILED at {}: partitionId:{}, offset:{}, processing a batch of given records [{}]",
                    Instant.now(), partitionId, offset, recordsString);
            // TODO: retry? OR write messages into some 'maas_metrics_error' topic, so that later on
            // we can read it from that error topic
        }

        LOGGER.debug("Done processing for records:{}", recordsString);

        // Reset the counter
        if(batchProcessedCount == Long.MAX_VALUE) batchProcessedCount = 0;

        if(batchProcessedCount % MESSAGE_PROCESS_REPORT_COUNT == 0) {
            LOGGER.info("Processed {} batches so far after start or reset...", getBatchProcessedCount());
        }

        return isSuccessful;
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> map, ConsumerSeekCallback consumerSeekCallback) {
        for(TopicPartition topicPartition : map.keySet()) {
            String topic = topicPartition.topic();
            int partition = topicPartition.partition();
            long offset = map.get(topicPartition);
            LOGGER.info("At Partition assignment for topic [{}], partition [{}], offset is at [{}] at time [{}]",
                    topic, partition, offset, Instant.now());
        }
    }

    @Override
    public void onIdleContainer(Map<TopicPartition, Long> map, ConsumerSeekCallback consumerSeekCallback) {
        LOGGER.info("Listener container is idle at [{}]", Instant.now());
    }

    /**
     * Get the current count of the total message processed by the consumer
     * @return
     */
    public long getBatchProcessedCount(){
        return batchProcessedCount;
    }
}
