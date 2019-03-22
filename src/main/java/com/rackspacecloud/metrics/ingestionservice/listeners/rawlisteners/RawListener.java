package com.rackspacecloud.metrics.ingestionservice.listeners.rawlisteners;

import com.rackspace.monplat.protocol.ExternalMetric;
import com.rackspacecloud.metrics.ingestionservice.influxdb.InfluxDBHelper;
import com.rackspacecloud.metrics.ingestionservice.listeners.UnifiedMetricsListener;
import com.rackspacecloud.metrics.ingestionservice.listeners.processors.TenantIdAndMeasurement;
import com.rackspacecloud.metrics.ingestionservice.listeners.rawlisteners.processors.RawMetricsProcessor;
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
    @KafkaListener(
            topics = "${kafka.topics.in}",
            containerFactory = "batchFactory",
            errorHandler = "listenerErrorHandler"
    )
    public void listenUnifiedMetricsTopic(
            @Payload final List<ExternalMetric> records,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partitionId,
            @Header(KafkaHeaders.OFFSET) final long offset,
            final Acknowledgment ack) throws Exception {

        batchProcessedCount++;

        // Prepare the payloads to ingest
        Map<TenantIdAndMeasurement, List<String>> tenantPayloadsMap =
                RawMetricsProcessor.getTenantPayloadsMap(partitionId, offset, records);

        writeIntoInfluxDb(tenantPayloadsMap);

        processPostInfluxDbIngestion(records.toString(), partitionId, offset, ack);
    }

    private void writeIntoInfluxDb(Map<TenantIdAndMeasurement, List<String>> tenantPayloadsMap) {

        for(Map.Entry<TenantIdAndMeasurement, List<String>> entry : tenantPayloadsMap.entrySet()) {
            TenantIdAndMeasurement tenantIdAndMeasurement = entry.getKey();
            String payload = String.join("\n", entry.getValue());
            try {
                // cleanup tenantIdAndMeasurement by replacing any special character
                // with "_" before passing it to the function
                influxDBHelper.ingestToInfluxDb(
                        payload, tenantIdAndMeasurement.getTenantId(),
                        tenantIdAndMeasurement.getMeasurement(), "full");
                // TODO: make enum for rollup level

            } catch (Exception e) {
                String msg = String.format("Write to InfluxDB failed with exception message [%s].", e.getMessage());
                LOGGER.error("[{}] Payload [{}]", msg, payload, e);

//                throw new Exception(msg, e);
            }
        }
    }
}
