package com.rackspacecloud.metrics.ingestionservice.listeners;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public class UnifiedMetricsListener implements ConsumerSeekAware {

    protected long batchProcessedCount = 0;

    // At the end of every 1000 messages, log this information
    protected static final int MESSAGE_PROCESS_REPORT_COUNT = 1000;

    private static final Logger LOGGER = LoggerFactory.getLogger(UnifiedMetricsListener.class);

    @Override
    public void registerSeekCallback(ConsumerSeekCallback consumerSeekCallback) {
        LOGGER.info("Registering seekCallback at [{}]", Instant.now());
    }

    protected void processPostInfluxDbIngestion(
            final List<?> records, final int partitionId,
            final long offset, final Acknowledgment ack) {

        ack.acknowledge();
        LOGGER.debug("Successfully processed partitionId:{}, offset:{} at {}", partitionId, offset, Instant.now());

        if (batchProcessedCount % MESSAGE_PROCESS_REPORT_COUNT == 0) {
            LOGGER.info("Processed {} batches.", batchProcessedCount);
        }

        LOGGER.debug("Done processing for records:{}", records);

        // Reset the counter
        if(batchProcessedCount == Long.MAX_VALUE) batchProcessedCount = 0;

        if(batchProcessedCount % MESSAGE_PROCESS_REPORT_COUNT == 0) {
            LOGGER.info("Processed {} batches so far after start or reset...", getBatchProcessedCount());
        }
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

    public static String replaceSpecialCharacters(String inputString){
        final String[] metaCharacters =
                {"\\",":","^","$","{","}","[","]","(",")",".","*","+","?","|","<",">","-","&","%"," "};

        for (int i = 0 ; i < metaCharacters.length ; i++){
            if(inputString.contains(metaCharacters[i])){
                inputString = inputString.replace(metaCharacters[i],"_");
            }
        }
        return inputString;
    }
}
