package com.rackspacecloud.metrics.ingestionservice.listeners.rolluplisteners;

import com.rackspacecloud.metrics.ingestionservice.exceptions.IngestFailedException;
import com.rackspacecloud.metrics.ingestionservice.influxdb.InfluxDBHelper;
import com.rackspacecloud.metrics.ingestionservice.listeners.UnifiedMetricsListener;
import com.rackspacecloud.metrics.ingestionservice.listeners.processors.TenantIdAndMeasurement;
import com.rackspacecloud.metrics.ingestionservice.listeners.rolluplisteners.models.MetricRollup;
import com.rackspacecloud.metrics.ingestionservice.listeners.rolluplisteners.processors.MetricsRollupProcessor;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
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

@Slf4j
public class RollupListener extends UnifiedMetricsListener {
    private InfluxDBHelper influxDBHelper;

    @Value("${tenant-routing-service.url}")
    protected static String tenantRoutingServiceUrl;

    @Autowired
    public RollupListener(InfluxDBHelper influxDBHelper) {
        this.influxDBHelper = influxDBHelper;
    }

    /**
     * This listener listens to unified.metrics.json topic.
     * @param records
     */
    @KafkaListener(
            topics = "${kafka.topics.source-5m}",
            containerFactory = "batchFactory",
            errorHandler = "listenerErrorHandler"
    )
    public void listenMetricsRollup5m(
            @Payload final List<MetricRollup> records,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partitionId,
            @Header(KafkaHeaders.OFFSET) final long offset,
            final Acknowledgment ack) throws IngestFailedException {

        String rollupLevel = "5m";

        listenMetricsRollup(records, partitionId, offset, ack, rollupLevel);
    }

    @KafkaListener(
            topics = "${kafka.topics.source-20m}",
            containerFactory = "batchFactory",
            errorHandler = "listenerErrorHandler"
    )
    public void listenMetricsRollup20m(
            @Payload final List<MetricRollup> records,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partitionId,
            @Header(KafkaHeaders.OFFSET) final long offset,
            final Acknowledgment ack) throws IngestFailedException {

        String rollupLevel = "20m";

        listenMetricsRollup(records, partitionId, offset, ack, rollupLevel);
    }

    @KafkaListener(
            topics = "${kafka.topics.source-60m}",
            containerFactory = "batchFactory",
            errorHandler = "listenerErrorHandler"
    )
    public void listenMetricsRollup60m(
            @Payload final List<MetricRollup> records,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partitionId,
            @Header(KafkaHeaders.OFFSET) final long offset,
            final Acknowledgment ack) throws IngestFailedException {

        String rollupLevel = "60m";

        listenMetricsRollup(records, partitionId, offset, ack, rollupLevel);
    }

    @KafkaListener(
            topics = "${kafka.topics.source-240m}",
            containerFactory = "batchFactory",
            errorHandler = "listenerErrorHandler"
    )
    public void listenMetricsRollup240m(
            @Payload final List<MetricRollup> records,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partitionId,
            @Header(KafkaHeaders.OFFSET) final long offset,
            final Acknowledgment ack) throws IngestFailedException {

        String rollupLevel = "240m";

        listenMetricsRollup(records, partitionId, offset, ack, rollupLevel);
    }

    @KafkaListener(
            topics = "${kafka.topics.source-1440m}",
            containerFactory = "batchFactory",
            errorHandler = "listenerErrorHandler"
    )
    public void listenMetricsRollup1440m(
            @Payload final List<MetricRollup> records,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partitionId,
            @Header(KafkaHeaders.OFFSET) final long offset,
            final Acknowledgment ack) throws IngestFailedException {

        String rollupLevel = "1440m";

        listenMetricsRollup(records, partitionId, offset, ack, rollupLevel);
    }

    private void listenMetricsRollup(
            @Payload List<MetricRollup> records,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partitionId,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack,
            String rollupLevel) throws IngestFailedException {

        batchProcessedCount++;

        Map<TenantIdAndMeasurement, List<String>> tenantPayloadsMap =
                MetricsRollupProcessor.getTenantRollupPayloadsMap(partitionId, offset, records);

        writeToInfluxDb(tenantPayloadsMap, rollupLevel);

        processPostInfluxDbIngestion(records, ack);
    }

    private void writeToInfluxDb(
            final Map<TenantIdAndMeasurement, List<String>> tenantPayloadsMap,
            final String rollupLevel) throws IngestFailedException {

        for(Map.Entry<TenantIdAndMeasurement, List<String>> entry : tenantPayloadsMap.entrySet()) {
            TenantIdAndMeasurement tenantIdAndMeasurement = entry.getKey();
            String payload = String.join("\n", entry.getValue());
            try {
                // cleanup tenantIdAndMeasurement by replacing any special character with "_" before passing it to the function
                influxDBHelper.ingestToInfluxDb(
                        payload, tenantIdAndMeasurement.getTenantId(),
                        tenantIdAndMeasurement.getMeasurement(), rollupLevel);

            } catch (IngestFailedException e) {
                String msg = String.format("Write to InfluxDB failed with exception message [%s].", e.getMessage());
                if(e.getCause().getClass().equals(ResourceAccessException.class)){
                    log.error(msg, e);
                } else {
                    log.error("[{}] Payload [{}]", msg, payload, e);
                }
                throw e;
            }
        }
    }
}
