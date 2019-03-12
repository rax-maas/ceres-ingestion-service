package com.rackspacecloud.metrics.ingestionservice.listeners.rolluplisteners.processors;

import com.rackspacecloud.metrics.ingestionservice.influxdb.Point;
import com.rackspacecloud.metrics.ingestionservice.listeners.processors.Dimension;
import com.rackspacecloud.metrics.ingestionservice.listeners.processors.TenantIdAndMeasurement;
import com.rackspacecloud.metrics.ingestionservice.listeners.rolluplisteners.models.MetricRollup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.rackspacecloud.metrics.ingestionservice.utils.InfluxDBUtils.replaceSpecialCharacters;

/**
 * This class processes MetricsRollup (JSON) message from Kafka into InfluxDB formatted line-protocol string
 */
public class MetricsRollupProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsRollupProcessor.class);
    private static final String TENANT_ID = "tenantIdAndMeasurement";
    private static final String CHECK_TYPE = "checkType";

    private static Dimension getDimensions(MetricRollup record) {
        Dimension dimension = new Dimension();
        dimension.setCollectionLabel(record.getCollectionLabel());
        dimension.setCollectionTarget(record.getCollectionTarget());
        dimension.setDeviceLabel(record.getDeviceLabel());
        dimension.setMonitoringSystem(record.getMonitoringSystem());
        dimension.setSystemMetadata(record.getSystemMetadata());

        return dimension;
    }

    /**
     * This method creates the payloads to be written into InfluxDB
     * @param partitionId
     * @param offset
     * @param records
     * @return
     */
    public static final Map<TenantIdAndMeasurement, List<String>> getTenantRollupPayloadsMap(
            int partitionId, long offset, List<MetricRollup> records) {

        Map<TenantIdAndMeasurement, List<String>> tenantPayloadMap = new HashMap<>();
        int numberOfRecordsNotConvertedIntoInfluxDBPoints = 0;

        for(MetricRollup record : records) {
            LOGGER.debug("Received partitionId:{}; Offset:{}; record:{}", partitionId, offset, record);

            String tenantId = record.getSystemMetadata().get(TENANT_ID);
            if (!isValid(TENANT_ID, tenantId, record)) {
                LOGGER.error("Invalid tenant ID [{}] in the received record [{}]", tenantId, record);
                throw new IllegalArgumentException(String.format("Invalid tenant Id: [%s]", tenantId));
            }

            String measurement = record.getSystemMetadata().get(CHECK_TYPE);
            if (!isValid(CHECK_TYPE, measurement, record)) {
                LOGGER.error("Invalid measurement [{}] in the received record [{}]", measurement, record);
                throw new IllegalArgumentException(String.format("Invalid measurement: [%s]", measurement));
            }

            Dimension dimension = getDimensions(record);

            try {
                Point.Builder pointBuilder = Dimension.populateTagsAndFields(dimension);
                populatePayload(record, pointBuilder);
                Point point =  pointBuilder.build();

                TenantIdAndMeasurement tenantIdAndMeasurement = new TenantIdAndMeasurement(tenantId, measurement);

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

    private static void populatePayload(final MetricRollup record, final Point.Builder pointBuilder) {
        pointBuilder.addField("start", record.getStart());
        pointBuilder.addField("end", record.getEnd());

        for(Map.Entry<String, MetricRollup.RollupBucket<Long>> entry : record.getIvalues().entrySet()){
            String metricFieldName = replaceSpecialCharacters(entry.getKey());
            pointBuilder.tag(String.format("%s_unit", metricFieldName), record.getUnits().get(entry.getKey()));

            MetricRollup.RollupBucket<Long> rollupBucketLong = entry.getValue();
            pointBuilder.addField(
                    String.format("%s_%s", metricFieldName, "min"),
                    rollupBucketLong.getMin().doubleValue());

            pointBuilder.addField(
                    String.format("%s_%s", metricFieldName, "mean"),
                    rollupBucketLong.getMean().doubleValue());

            pointBuilder.addField(
                    String.format("%s_%s", metricFieldName, "max"),
                    rollupBucketLong.getMax().doubleValue());
        }

        for(Map.Entry<String, MetricRollup.RollupBucket<Double>> entry : record.getFvalues().entrySet()){
            String metricFieldName = replaceSpecialCharacters(entry.getKey());
            pointBuilder.tag(String.format("%s_unit", metricFieldName), record.getUnits().get(entry.getKey()));

            MetricRollup.RollupBucket<Double> rollupBucketDouble = entry.getValue();

            pointBuilder.addField(
                    String.format("%s_%s", metricFieldName, "min"),
                    rollupBucketDouble.getMin().doubleValue());

            pointBuilder.addField(
                    String.format("%s_%s", metricFieldName, "mean"),
                    rollupBucketDouble.getMean().doubleValue());

            pointBuilder.addField(
                    String.format("%s_%s", metricFieldName, "max"),
                    rollupBucketDouble.getMax().doubleValue());
        }
    }

    private static boolean isValid(String fieldName, CharSequence fieldValue, MetricRollup record){
        if(StringUtils.isEmpty(fieldValue)){
            LOGGER.error("There is no value for the field '{}' in record [{}]", fieldName, record);
            return false;
        }
        // TenantId validation to make sure it contains only alphanum and ‘:’
        else if(fieldName.equals(TENANT_ID) && !(fieldValue.toString()).matches("^[a-zA-Z0-9:]*$")){
            LOGGER.error("Invalid tenantIdAndMeasurement '{}' found.", fieldValue);
            return false;
        }

        return true;
    }
}
