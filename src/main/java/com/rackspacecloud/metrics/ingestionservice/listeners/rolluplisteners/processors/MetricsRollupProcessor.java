package com.rackspacecloud.metrics.ingestionservice.listeners.rolluplisteners.processors;

import com.rackspacecloud.metrics.ingestionservice.listeners.UnifiedMetricsListener;
import com.rackspacecloud.metrics.ingestionservice.listeners.processors.CommonMetricsProcessor;
import com.rackspacecloud.metrics.ingestionservice.listeners.processors.Dimension;
import com.rackspacecloud.metrics.ingestionservice.listeners.processors.TenantIdAndMeasurement;
import com.rackspacecloud.metrics.ingestionservice.listeners.rolluplisteners.models.MetricRollup;
import lombok.extern.slf4j.Slf4j;
import org.influxdb.dto.Point;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * This class processes MetricsRollup (JSON) message from Kafka into InfluxDB formatted line-protocol string
 */
@Slf4j
public class MetricsRollupProcessor {    

    private static Dimension getDimensions(MetricRollup record) {
        Dimension dimension = new Dimension();
        dimension.setAccountType(record.getAccountType());
        dimension.setAccount(record.getAccount());
        dimension.setDevice(record.getDevice());
        dimension.setDeviceLabel(record.getDeviceLabel());
        dimension.setDeviceMetadata(record.getDeviceMetadata());
        dimension.setMonitoringSystem(record.getMonitoringSystem());
        dimension.setSystemMetadata(record.getSystemMetadata());
        dimension.setCollectionName(record.getCollectionName());
        dimension.setCollectionLabel(record.getCollectionLabel());
        dimension.setCollectionTarget(record.getCollectionTarget());
        dimension.setCollectionMetadata(record.getCollectionMetadata());

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

        ConcurrentMap<TenantIdAndMeasurement, List<String>> tenantPayloadMap = new ConcurrentHashMap<>();
        int numberOfRecordsNotConvertedIntoInfluxDBPoints = 0;

        for(MetricRollup record : records) {
            log.debug("Received partitionId:{}; Offset:{}; record:{}", partitionId, offset, record);

            String accountType = record.getAccountType();
            String account = record.getAccount();
            String monitoringSystem = record.getMonitoringSystem();
            String collectionName = record.getCollectionName();

            TenantIdAndMeasurement tenantIdAndMeasurement =
                    CommonMetricsProcessor.getTenantIdAndMeasurement(
                            accountType, account, monitoringSystem, collectionName);

            Dimension dimension = getDimensions(record);

            try {
                Point.Builder pointBuilder = Dimension.populateTagsAndFields(dimension, tenantIdAndMeasurement);
                populatePayload(record, pointBuilder);
                Point point =  pointBuilder.build();

                List<String> payloads = tenantPayloadMap.computeIfAbsent(tenantIdAndMeasurement,
                        key -> Collections.synchronizedList(new ArrayList<>()));

                synchronized (payloads) {
                    payloads.add(point.lineProtocol(TimeUnit.SECONDS));
                }
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

    private static void populatePayload(final MetricRollup record, final Point.Builder pointBuilder) {
        pointBuilder.addField("start", record.getStart());
        pointBuilder.addField("end", record.getEnd());

        for(Map.Entry<String, MetricRollup.RollupBucket<Long>> entry : record.getIvalues().entrySet()){
            String metricFieldName = UnifiedMetricsListener.replaceSpecialCharacters(entry.getKey());
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
            String metricFieldName = UnifiedMetricsListener.replaceSpecialCharacters(entry.getKey());
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
}
