package com.rackspacecloud.metrics.ingestionservice.listeners.rolluplisteners;

import com.rackspacecloud.metrics.ingestionservice.influxdb.InfluxDBHelper;
import com.rackspacecloud.metrics.ingestionservice.listeners.UnifiedMetricsListener;
import com.rackspacecloud.metrics.ingestionservice.listeners.rolluplisteners.models.MetricRollup;
import com.rackspacecloud.metrics.ingestionservice.listeners.rolluplisteners.processors.MetricsRollupProcessor;
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
import org.springframework.web.client.ResourceAccessException;

import java.util.List;
import java.util.Map;

public class RollupListener extends UnifiedMetricsListener {
    private InfluxDBHelper influxDBHelper;

    private static final Logger LOGGER = LoggerFactory.getLogger(RollupListener.class);

    @Value("${tenant-routing-service.url}")
    protected static String tenantRoutingServiceUrl;

    @Autowired
    public RollupListener(InfluxDBHelper influxDBHelper, MeterRegistry registry) {
        super(registry);
        this.influxDBHelper = influxDBHelper;
    }

    /**
     * This listener listens to unified.metrics.json topic.
     *
     * @param records
     */
    @KafkaListener(topics = "${kafka.topics.source-5m}", containerFactory = "batchFactory", errorHandler = "listenerErrorHandler")
    public boolean listenMetricsRollup5m(@Payload final List<MetricRollup> records,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partitionId,
            @Header(KafkaHeaders.OFFSET) final long offset, final Acknowledgment ack) throws Exception {

        String rollupLevel = "5m";

        return listenMetricsRollup(records, partitionId, offset, ack, rollupLevel);
    }

    @KafkaListener(topics = "${kafka.topics.source-20m}", containerFactory = "batchFactory", errorHandler = "listenerErrorHandler")
    public boolean listenMetricsRollup20m(@Payload final List<MetricRollup> records,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partitionId,
            @Header(KafkaHeaders.OFFSET) final long offset, final Acknowledgment ack) throws Exception {

        String rollupLevel = "20m";

        return listenMetricsRollup(records, partitionId, offset, ack, rollupLevel);
    }

    @KafkaListener(topics = "${kafka.topics.source-60m}", containerFactory = "batchFactory", errorHandler = "listenerErrorHandler")
    public boolean listenMetricsRollup60m(@Payload final List<MetricRollup> records,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partitionId,
            @Header(KafkaHeaders.OFFSET) final long offset, final Acknowledgment ack) throws Exception {

        String rollupLevel = "60m";

        return listenMetricsRollup(records, partitionId, offset, ack, rollupLevel);
    }

    @KafkaListener(topics = "${kafka.topics.source-240m}", containerFactory = "batchFactory", errorHandler = "listenerErrorHandler")
    public boolean listenMetricsRollup240m(@Payload final List<MetricRollup> records,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partitionId,
            @Header(KafkaHeaders.OFFSET) final long offset, final Acknowledgment ack) throws Exception {

        String rollupLevel = "240m";

        return listenMetricsRollup(records, partitionId, offset, ack, rollupLevel);
    }

    @KafkaListener(topics = "${kafka.topics.source-1440m}", containerFactory = "batchFactory", errorHandler = "listenerErrorHandler")
    public boolean listenMetricsRollup1440m(@Payload final List<MetricRollup> records,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partitionId,
            @Header(KafkaHeaders.OFFSET) final long offset, final Acknowledgment ack) throws Exception {

        String rollupLevel = "1440m";

        return listenMetricsRollup(records, partitionId, offset, ack, rollupLevel);
    }

    private boolean listenMetricsRollup(@Payload List<MetricRollup> records,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partitionId, @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack, String rollupLevel) throws Exception {

        batchProcessedCount++;

        Map<String, List<String>> tenantPayloadsMap = MetricsRollupProcessor.getTenantRollupPayloadsMap(partitionId,
                offset, records);

        boolean isInfluxdbIngestionSuccessful = writeToInfluxDb(tenantPayloadsMap, rollupLevel);

        return processPostInfluxDbIngestion(records.toString(), partitionId, offset, ack,
                isInfluxdbIngestionSuccessful);
    }

    private boolean writeToInfluxDb(final Map<String, List<String>> tenantPayloadsMap, final String rollupLevel)
            throws Exception {

        boolean isInfluxdbIngestionSuccessful = false;

        for (Map.Entry<String, List<String>> entry : tenantPayloadsMap.entrySet()) {
            String tenantId = entry.getKey();
            String payload = String.join("\n", entry.getValue());
            try {
                // cleanup tenantId by replacing any special character with "_" before passing
                // it to the function
                isInfluxdbIngestionSuccessful = influxDBHelper.ingestToInfluxDb(payload,
                        replaceSpecialCharacters(tenantId), rollupLevel);

            } catch (Exception e) {
                String msg = String.format("Write to InfluxDB failed with exception message [%s].", e.getMessage());
                if (e.getCause().getClass().equals(ResourceAccessException.class)) {
                    LOGGER.error(msg, e);
                } else {
                    LOGGER.error("[{}] Payload [{}]", msg, payload, e);
                }

                throw new Exception(msg, e);
            }

            if (!isInfluxdbIngestionSuccessful)
                break;
        }
        return isInfluxdbIngestionSuccessful;
    }
}