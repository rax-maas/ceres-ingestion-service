package com.rackspacecloud.metrics.ingestionservice.listeners.rawlisteners.processors;

import com.rackspace.monplat.protocol.ExternalMetric;
import com.rackspacecloud.metrics.ingestionservice.influxdb.Point;
import com.rackspacecloud.metrics.ingestionservice.listeners.processors.CommonMetricsProcessor;
import com.rackspacecloud.metrics.ingestionservice.listeners.processors.Dimension;
import com.rackspacecloud.metrics.ingestionservice.listeners.processors.TenantIdAndMeasurement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.rackspacecloud.metrics.ingestionservice.utils.InfluxDBUtils.replaceSpecialCharacters;

public class RawMetricsProcessor {

    private static final String TIMESTAMP = "timestamp";
    private static final String UNAVAILABLE = "unavailable";

    private static final Logger LOGGER = LoggerFactory.getLogger(RawMetricsProcessor.class);

    public static final Map<TenantIdAndMeasurement, List<String>> getTenantPayloadsMap(
            int partitionId, long offset, List<ExternalMetric> records) throws Exception {

        Map<TenantIdAndMeasurement, List<String>> tenantPayloadMap = new HashMap<>();
        int numberOfRecordsNotConvertedIntoInfluxDBPoints = 0;

        for(ExternalMetric record : records) {
            LOGGER.debug("Received partitionId:{}; Offset:{}; record:{}", partitionId, offset, record);

            if(!CommonMetricsProcessor.isValid(TIMESTAMP, record.getTimestamp()))
                throw new Exception("Invalid timestamp [" + record.getTimestamp() + "]");

            Dimension dimension = CommonMetricsProcessor.getDimensions(record);

            try {
                String accountType = record.getAccountType().name();
                String account = record.getAccount();
                String monitoringSystem = record.getMonitoringSystem().name();
                String collectionName = record.getCollectionName();

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
                LOGGER.error("Can't convert message into InfluxDB Point. Faulty Message is: [{}]", record);
            }
        }

        if(numberOfRecordsNotConvertedIntoInfluxDBPoints > 0) {
            LOGGER.info("Out of [{}] messages in this batch [{}] couldn't convert into InfluxDB Points.",
                    records.size(), numberOfRecordsNotConvertedIntoInfluxDBPoints);
        }

        return tenantPayloadMap;
    }

    static void populatePayload(final ExternalMetric record, final Point.Builder pointBuilder) {
        for(Map.Entry<String, Long> entry : record.getIvalues().entrySet()){
            String iKey = entry.getKey();
            String metricFieldName = replaceSpecialCharacters(iKey);
            String unitValue = record.getUnits().get(iKey);

            pointBuilder.tag(String.format("%s_unit", metricFieldName), unitValue == null ? UNAVAILABLE : unitValue);
            pointBuilder.addField(metricFieldName, entry.getValue().doubleValue());
        }

        for(Map.Entry<String, Double> entry : record.getFvalues().entrySet()){
            String fKey = entry.getKey();
            String metricFieldName = replaceSpecialCharacters(fKey);
            String unitValue = record.getUnits().get(fKey);

            pointBuilder.tag(String.format("%s_unit", metricFieldName), unitValue == null ? UNAVAILABLE : unitValue);
            pointBuilder.addField(metricFieldName, entry.getValue());
        }
    }
}
