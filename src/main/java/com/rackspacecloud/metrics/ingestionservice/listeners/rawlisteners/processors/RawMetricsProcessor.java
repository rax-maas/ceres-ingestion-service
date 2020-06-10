package com.rackspacecloud.metrics.ingestionservice.listeners.rawlisteners.processors;

import com.rackspace.monplat.protocol.ExternalMetric;
import com.rackspacecloud.metrics.ingestionservice.exceptions.InvalidDataException;
import com.rackspacecloud.metrics.ingestionservice.listeners.UnifiedMetricsListener;
import com.rackspacecloud.metrics.ingestionservice.listeners.processors.CommonMetricsProcessor;
import com.rackspacecloud.metrics.ingestionservice.listeners.processors.Dimension;
import com.rackspacecloud.metrics.ingestionservice.listeners.processors.TenantIdAndMeasurement;
import lombok.extern.slf4j.Slf4j;
import org.influxdb.dto.Point;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RawMetricsProcessor {

    private static final String TIMESTAMP = "timestamp";
    private static final String UNAVAILABLE = "unavailable";

    public static final Map<TenantIdAndMeasurement, List<String>> getTenantPayloadsMap(
            List<Message<ExternalMetric>> records,
            ConcurrentMap<String, Map<String, Map<String, Long>>> topicPartitionRecordsCount)
        throws InvalidDataException {

        Map<TenantIdAndMeasurement, List<String>> tenantPayloadMap = new HashMap<>();
        int numberOfRecordsNotConvertedIntoInfluxDBPoints = 0;

        for(Message<ExternalMetric> message : records) {
            ExternalMetric record = message.getPayload();
            MessageHeaders headers = message.getHeaders();

            populateHeaderMetrics(topicPartitionRecordsCount, record, headers);

            if(!CommonMetricsProcessor.isValid(TIMESTAMP, record.getTimestamp()))
                throw new InvalidDataException("Invalid timestamp [" + record.getTimestamp() + "]");

            // Get all of the tags into Dimension
            Dimension dimension = CommonMetricsProcessor.getDimensions(record);

            try {
                String accountType = dimension.getAccountType();
                String account = dimension.getAccount();
                String monitoringSystem = dimension.getMonitoringSystem();
                String collectionName = dimension.getCollectionName();

                TenantIdAndMeasurement tenantIdAndMeasurement =
                        CommonMetricsProcessor.getTenantIdAndMeasurement(
                                accountType, account, monitoringSystem, collectionName);

                Point.Builder pointBuilder = Dimension.populateTagsAndFields(dimension, tenantIdAndMeasurement);
                populatePayload(record, pointBuilder);

                Instant instant = Instant.parse(record.getTimestamp());
                pointBuilder.time(instant.getEpochSecond(), TimeUnit.SECONDS);

                Point point = pointBuilder.build();

                if (!tenantPayloadMap.containsKey(tenantIdAndMeasurement))
                    tenantPayloadMap.put(tenantIdAndMeasurement, new ArrayList<>());

                List<String> payloads = tenantPayloadMap.get(tenantIdAndMeasurement);
                payloads.add(point.lineProtocol(TimeUnit.SECONDS));
            }
            catch (Exception ex) {
                numberOfRecordsNotConvertedIntoInfluxDBPoints++;
                log.error("Can't convert message into InfluxDB Point. Faulty Message is: [{}]", record);
            }
        }

        if(numberOfRecordsNotConvertedIntoInfluxDBPoints > 0) {
            log.info("Out of [{}] messages in this batch [{}] couldn't convert into InfluxDB Points.",
                    records.size(), numberOfRecordsNotConvertedIntoInfluxDBPoints);
        }

        return tenantPayloadMap;
    }

    private static void populateHeaderMetrics(
            ConcurrentMap<String, Map<String, Map<String, Long>>> topicPartitionRecordsCount,
            ExternalMetric record, MessageHeaders headers) {

        String topic = headers.get(KafkaHeaders.RECEIVED_TOPIC).toString();
        String partitionId = headers.get(KafkaHeaders.RECEIVED_PARTITION_ID).toString();
        String offset = headers.get(KafkaHeaders.OFFSET).toString();
        String monitoringSystem = record.getMonitoringSystem().name();

        // Initialize if topic doesn't exist in the map
        topicPartitionRecordsCount.computeIfAbsent(topic, k -> {

            // Initialize for the partitionID map
            Map<String, Map<String, Long>> partitionMonitoringSystemRecordsCount = new HashMap<>();
            partitionMonitoringSystemRecordsCount.put(partitionId, new HashMap<>());

            Map<String, Long> monitoringSystemRecordsCount = partitionMonitoringSystemRecordsCount.get(partitionId);

            // Initialize the record count for given monitoring system (i.e. MAAS, UIM, SALUS, etc)
            monitoringSystemRecordsCount.put(monitoringSystem, 0L);
            return partitionMonitoringSystemRecordsCount;
        });

        // Update the record count when "topic" is already present in the map
        topicPartitionRecordsCount.computeIfPresent(topic, (t, partitionRecordsCountMap) -> {

            // Seeing this particular partitionId first time
            if(!partitionRecordsCountMap.containsKey(partitionId)) {
                partitionRecordsCountMap.put(partitionId, new HashMap<>()); // Initialize map for partitionId

                Map<String, Long> monitoringSystemRecordsCount = partitionRecordsCountMap.get(partitionId);
                monitoringSystemRecordsCount.put(monitoringSystem, 0L);
            }
            else {
                Map<String, Long> monitoringSystemRecordsCount = partitionRecordsCountMap.get(partitionId);

                // Topic and partitionId exist, but monitoring system does not exist. So, initialize its map
                if(!monitoringSystemRecordsCount.containsKey(monitoringSystem)) {
                    monitoringSystemRecordsCount.put(monitoringSystem, 0L);
                }
            }

            Map<String, Long> monitoringSystemCountMap = partitionRecordsCountMap.get(partitionId);
            long count = monitoringSystemCountMap.get(monitoringSystem);
            monitoringSystemCountMap.put(monitoringSystem, ++count);
            return partitionRecordsCountMap;
        });

        log.debug("Received topic:{}; partitionId:{}; Offset:{}; record:{}", topic, partitionId, offset, record);
    }

    static void populatePayload(final ExternalMetric record, final Point.Builder pointBuilder) {
        for(Map.Entry<String, Long> entry : record.getIvalues().entrySet()){
            String iKey = entry.getKey();
            String metricFieldName = UnifiedMetricsListener.replaceSpecialCharacters(iKey);
            String unitValue = record.getUnits().get(iKey);

            pointBuilder.tag(String.format("%s_unit", metricFieldName), unitValue == null ? UNAVAILABLE : unitValue);
            pointBuilder.addField(metricFieldName, entry.getValue().doubleValue());
        }

        for(Map.Entry<String, Double> entry : record.getFvalues().entrySet()){
            String fKey = entry.getKey();
            String metricFieldName = UnifiedMetricsListener.replaceSpecialCharacters(fKey);
            String unitValue = record.getUnits().get(fKey);

            pointBuilder.tag(String.format("%s_unit", metricFieldName), unitValue == null ? UNAVAILABLE : unitValue);
            pointBuilder.addField(metricFieldName, entry.getValue());
        }
    }
}
