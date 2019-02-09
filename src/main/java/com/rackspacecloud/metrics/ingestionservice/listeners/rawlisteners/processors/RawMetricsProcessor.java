package com.rackspacecloud.metrics.ingestionservice.listeners.rawlisteners.processors;

import com.rackspace.maas.model.Metric;
import com.rackspacecloud.metrics.ingestionservice.influxdb.Point;
import com.rackspacecloud.metrics.ingestionservice.listeners.processors.Dimension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

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
    private static final String TENANT_ID = "tenantId";

    private static Dimension getDimensions(Metric record) {
        Dimension dimension = new Dimension();
        dimension.setCollectionLabel(record.getCollectionLabel());
        dimension.setCollectionTarget(record.getCollectionTarget());
        dimension.setDeviceLabel(record.getDeviceLabel());
        dimension.setMonitoringSystem(record.getMonitoringSystem().name());
        dimension.setSystemMetadata(record.getSystemMetadata());

        return dimension;
    }

    public static final Map<String, List<String>> getTenantPayloadsMap(
            int partitionId, long offset, List<Metric> records) throws Exception {

        Map<String, List<String>> tenantPayloadMap = new HashMap<>();
        int numberOfRecordsNotConvertedIntoInfluxDBPoints = 0;

        for(Metric record : records) {
            LOGGER.debug("Received partitionId:{}; Offset:{}; record:{}", partitionId, offset, record);

            if(!isValid(TIMESTAMP, record.getTimestamp()))
                throw new Exception("Invalid timestamp [" + record.getTimestamp() + "]");

            Dimension dimension = getDimensions(record);

            try {
                String tenantId = dimension.getSystemMetadata().get(TENANT_ID);
                if (!isValid(TENANT_ID, tenantId)) {
                    LOGGER.error("Invalid tenant ID [{}] in the received record [{}]", tenantId, record);
                    throw new IllegalArgumentException(String.format("Invalid tenant Id: [%s]", tenantId));
                }

                Point.Builder pointBuilder = Dimension.populateTagsAndFields(dimension);
                populatePayload(record, pointBuilder);

                Instant instant = Instant.parse(record.getTimestamp());
                pointBuilder.time(instant.getEpochSecond(), TimeUnit.SECONDS);

                Point point = pointBuilder.build();

                if (!tenantPayloadMap.containsKey(tenantId)) tenantPayloadMap.put(tenantId, new ArrayList<>());

                List<String> payloads = tenantPayloadMap.get(tenantId);
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

    static void populatePayload(final Metric record, final Point.Builder pointBuilder) {
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

    static boolean isValid(String fieldName, CharSequence fieldValue){
        if(StringUtils.isEmpty(fieldValue)){
            LOGGER.error("There is no value for the field [{}]", fieldName);
            return false;
        }
        // TenantId validation to make sure it contains only alphanum and ‘:’
        else if(fieldName.equals(TENANT_ID) && !(fieldValue.toString()).matches("^[a-zA-Z0-9:]*$")){
            LOGGER.error("Invalid tenantId '{}' found.", fieldValue);
            return false;
        }

        return true;
    }
}
