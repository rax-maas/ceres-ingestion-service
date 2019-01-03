package com.rackspacecloud.metrics.ingestionservice;

import com.rackspace.maas.model.Metric;
import com.rackspacecloud.metrics.ingestionservice.influxdb.InfluxDBHelper;
import com.rackspacecloud.metrics.ingestionservice.processors.MetricsProcessor;
import com.rackspacecloud.metrics.ingestionservice.providers.IRouteProvider;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static com.rackspacecloud.metrics.ingestionservice.utils.InfluxDBUtils.replaceSpecialCharacters;

@Component
public class UnifiedMetricsListener implements ConsumerSeekAware {
    private InfluxDBHelper influxDBHelper;

    private long batchProcessedCount = 0;

    // At the end of every 1000 messages, log this information
    private static final int MESSAGE_PROCESS_REPORT_COUNT = 1000;

    private static final Logger LOGGER = LoggerFactory.getLogger(UnifiedMetricsListener.class);

    @Value("${tenant-routing-service.url}")
    protected static String tenantRoutingServiceUrl;

    @Autowired
    public UnifiedMetricsListener(RestTemplate restTemplate, IRouteProvider routeProvider){
        this.influxDBHelper = new InfluxDBHelper(restTemplate, routeProvider);
    }

    /**
     * This listener listens to unified.metrics.json topic.
     * @param records
     */
    @KafkaListener(topics = "${kafka.topics.in}", containerFactory = "batchFactory")
    public boolean listenUnifiedMetricsTopic(
            @Payload final List<Metric> records,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partitionId,
            @Header(KafkaHeaders.OFFSET) final long offset,
            final Acknowledgment ack) {

        batchProcessedCount++;

        Map<String, List<String>> tenantPayloadsMap =
                MetricsProcessor.getTenantPayloadsMap(partitionId, offset, records);

        boolean isInfluxDbIngestionSuccessful = writeIntoInfluxDb(tenantPayloadsMap);

        if (isInfluxDbIngestionSuccessful) {
            ack.acknowledge();
            LOGGER.debug("Successfully processed partionId:{}, offset:{} at {}",
                    partitionId, offset, Instant.now());

            if (batchProcessedCount % MESSAGE_PROCESS_REPORT_COUNT == 0) {
                LOGGER.info("Processed {} batches.", batchProcessedCount);
            }
        } else {
            LOGGER.error("FAILED at {}: partitionId:{}, offset:{}, processing a batch of given records [{}]",
                    Instant.now(), partitionId, offset, records);
            // TODO: retry? OR write messages into some 'maas_metrics_error' topic, so that later on
            // we can read it from that error topic
        }

        LOGGER.debug("Done processing for records:{}", records);

        // Reset the counter
        if(batchProcessedCount == Long.MAX_VALUE) batchProcessedCount = 0;

        if(batchProcessedCount % MESSAGE_PROCESS_REPORT_COUNT == 0) {
            LOGGER.info("Processed {} batches so far after start or reset...", getBatchProcessedCount());
        }

        return isInfluxDbIngestionSuccessful;
    }

    private boolean writeIntoInfluxDb(Map<String, List<String>> tenantPayloadsMap) {
        // TODO: Check for it we may need to add retentionPolicy information to store in Redis database
        String retentionPolicyName = "rp_5d";
        String retentionPolicy = "5d";

        boolean isInfluxDbIngestionSuccessful = true;

        for(Map.Entry<String, List<String>> entry : tenantPayloadsMap.entrySet()) {
            String tenantId = entry.getKey();
            String payload = String.join("\n", entry.getValue());
            try {
                // cleanup tenantId by replacing any special character with "_" before passing it to the function
                isInfluxDbIngestionSuccessful = influxDBHelper.ingestToInfluxdb(
                        payload, replaceSpecialCharacters(tenantId), retentionPolicy, retentionPolicyName);

            } catch (Exception e) {
                isInfluxDbIngestionSuccessful = false;
                LOGGER.error("Ingest failed for payload [{}] with exception message [{}]", payload, e.getMessage());
            }

            if(!isInfluxDbIngestionSuccessful) break;
        }
        return isInfluxDbIngestionSuccessful;
    }

    /**
     * Get the current count of the total message processed by the consumer
     * @return
     */
    public long getBatchProcessedCount(){
        return batchProcessedCount;
    }

    @Override
    public void registerSeekCallback(ConsumerSeekCallback consumerSeekCallback) {
        LOGGER.info("Registering seekCallback at [{}]", Instant.now());
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
}
