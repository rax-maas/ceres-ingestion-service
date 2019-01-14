package com.rackspacecloud.metrics.ingestionservice.listeners.rawlisteners;

import com.rackspace.maas.model.Metric;
import com.rackspacecloud.metrics.ingestionservice.influxdb.InfluxDBHelper;
import com.rackspacecloud.metrics.ingestionservice.listeners.UnifiedMetricsListener;
import com.rackspacecloud.metrics.ingestionservice.listeners.rawlisteners.processors.MetricsProcessor;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import java.util.List;
import java.util.Map;

import static com.rackspacecloud.metrics.ingestionservice.utils.InfluxDBUtils.replaceSpecialCharacters;

public class RawListener extends UnifiedMetricsListener {
    private InfluxDBHelper influxDBHelper;

    private static final Logger LOGGER = LoggerFactory.getLogger(RawListener.class);

    @Value("${tenant-routing-service.url}")
    protected static String tenantRoutingServiceUrl;

    @Autowired
    public RawListener(InfluxDBHelper influxDBHelper, MeterRegistry registry) {
        super(registry);
        this.influxDBHelper = influxDBHelper;
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

        counter.increment();

        batchProcessedCount++;

        Map<String, List<String>> tenantPayloadsMap =
                MetricsProcessor.getTenantPayloadsMap(partitionId, offset, records);

        boolean isInfluxDbIngestionSuccessful = writeIntoInfluxDb(tenantPayloadsMap);

        return processPostInfluxDbIngestion(records.toString(),
                partitionId, offset, ack, isInfluxDbIngestionSuccessful);
    }

    private boolean writeIntoInfluxDb(Map<String, List<String>> tenantPayloadsMap) {
        boolean isInfluxDbIngestionSuccessful = true;

        for(Map.Entry<String, List<String>> entry : tenantPayloadsMap.entrySet()) {
            String tenantId = entry.getKey();
            String payload = String.join("\n", entry.getValue());
            try {
                // cleanup tenantId by replacing any special character with "_" before passing it to the function
                isInfluxDbIngestionSuccessful = influxDBHelper.ingestToInfluxDb(
                        payload, replaceSpecialCharacters(tenantId), "full");
                // TODO: make enum for rollup level

            } catch (Exception e) {
                isInfluxDbIngestionSuccessful = false;
                LOGGER.error("Ingest failed for payload [{}] with exception message [{}]", payload, e.getMessage());
            }

            if(!isInfluxDbIngestionSuccessful) break;
        }
        return isInfluxDbIngestionSuccessful;
    }
}
