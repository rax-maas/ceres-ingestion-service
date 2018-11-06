package com.rackspacecloud.metrics.kafkainfluxdbconsumer;

import com.rackspace.maas.model.Metric;
import com.rackspacecloud.metrics.kafkainfluxdbconsumer.influxdb.InfluxDBHelper;
import com.rackspacecloud.metrics.kafkainfluxdbconsumer.influxdb.Point;
import com.rackspacecloud.metrics.kafkainfluxdbconsumer.providers.IRouteProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Component
public class UnifiedMetricsListener {
    private InfluxDBHelper influxDBHelper;

    private long batchProcessedCount = 0;

    // At the end of every 1000 messages, log this information
    private static final int MESSAGE_PROCESS_REPORT_COUNT = 1000;

    private static final String TIMESTAMP = "timestamp";
    private static final String TENANT_ID = "tenantId";
    private static final String CHECK_TYPE = "checkType";
    private static final String ACCOUNT_ID = "accountId";
    private static final String MONITORING_ZONE = "monitoringZone";
    private static final String ENTITY_ID = "entityId";

    private static final Logger LOGGER = LoggerFactory.getLogger(UnifiedMetricsListener.class);

    @Value("${tenant-routing-service.url}")
    protected static String tenantRoutingServiceUrl;

    @Autowired
    public UnifiedMetricsListener(RestTemplate restTemplate, IRouteProvider routeProvider){
        this.influxDBHelper = new InfluxDBHelper(restTemplate, routeProvider);
    }

//    @KafkaListener(topicPartitions = @TopicPartition(topic = "${kafka.topics.in}",
//            partitionOffsets = {
//                    @PartitionOffset(
//                            partition = "${kafka.topics.partition}",
//                            initialOffset = "${kafka.topics.offset}"
//                    )}), containerFactory = "batchFactory")
//    public void listen(List<Metric> messages, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partitionId,
//                       @Header(KafkaHeaders.OFFSET) final long offset, Acknowledgment ack) {
//        LOGGER.debug("Received partitionId:; Offset:; record:{}", messages);
//        batchProcessedCount++;
//    }

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

        Map<String, List<String>> tenantPayloadsMap = getTenantPayloadsMap(partitionId, offset, records);

        // TODO: Check for it we may need to add retentionPolicy information to store in Redis database
        String retentionPolicyName = "rp_5d";
        String retentionPolicy = "5d";

        boolean isInfluxdbIngestionSuccessful = true;

        for(Map.Entry<String, List<String>> entry : tenantPayloadsMap.entrySet()) {
            String tenantId = entry.getKey();
            String payload = String.join("\n", entry.getValue());
            try {
                // cleanup tenantId by replacing any special character with "_" before passing it to the function
                isInfluxdbIngestionSuccessful = influxDBHelper.ingestToInfluxdb(
                        payload, replaceSpecialCharacters(tenantId), retentionPolicy, retentionPolicyName);

                if(!isInfluxdbIngestionSuccessful) break;
            } catch (Exception e) {
                LOGGER.error("Ingest failed for payload [{}] with exception message [{}]", payload, e.getMessage());
            }
        }

        if (isInfluxdbIngestionSuccessful) {
            ack.acknowledge();
            LOGGER.debug("Successfully processed partionId:{}, offset:{} at {}",
                    partitionId, offset, Instant.now());

            if (batchProcessedCount % MESSAGE_PROCESS_REPORT_COUNT == 0) {
                LOGGER.info("Processed {} batches.", batchProcessedCount);
            }
        } else {
            LOGGER.error("FAILED at {}: partionId:{}, offset:{}, processing a batch of given records [{}]",
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

        return isInfluxdbIngestionSuccessful;
    }

    private Map<String, List<String>> getTenantPayloadsMap(int partitionId, long offset, List<Metric> records){
        Map<String, List<String>> tenantPayloadMap = new HashMap<>();

        for(Metric record : records) {
            LOGGER.debug("Received partitionId:{}; Offset:{}; record:{}", partitionId, offset, record);

            String tenantId = record.getSystemMetadata().get(TENANT_ID);
            if (!isValid(TENANT_ID, tenantId, record)) {
                LOGGER.error("Invalid tenant ID [{}] in the received record [{}]", tenantId, record);
                throw new IllegalArgumentException(String.format("Invalid tenant Id: [%s]", tenantId));
            }

            Point point = convertToInfluxdbPoint(record);

            if(!tenantPayloadMap.containsKey(tenantId)) tenantPayloadMap.put(tenantId, new ArrayList<>());

            List<String> payloads = tenantPayloadMap.get(tenantId);
            payloads.add(point.lineProtocol(TimeUnit.SECONDS));
        }

        return tenantPayloadMap;
    }

    /**
     * Get the current count of the total message processed by the consumer
     * @return
     */
    public long getBatchProcessedCount(){
        return batchProcessedCount;
    }

    /**
     * Convert the received record into Influxdb ingest input format
     * @param record
     * @return
     */
    private Point convertToInfluxdbPoint(Metric record){

        if(!isValid(TIMESTAMP, record.getTimestamp(), record)) return null;

        if(record.getSystemMetadata().get(CHECK_TYPE) == null) return null;

        String measurement = record.getSystemMetadata().get(CHECK_TYPE);

        Point.Builder pointBuilder = Point.measurement(replaceSpecialCharacters(measurement));
        populateTagsAndFields(record, pointBuilder);

        Instant instant = Instant.parse(record.getTimestamp());

        populatePayload(record, pointBuilder);

        pointBuilder.time(instant.getEpochSecond(), TimeUnit.SECONDS);

        return pointBuilder.build();
    }

    private void populateTagsAndFields(Metric record, Point.Builder pointBuilder) {

        if(!StringUtils.isEmpty(record.getSystemMetadata().get(ACCOUNT_ID))) {
            pointBuilder.tag("systemaccountid",
                    escapeSpecialCharactersForInfluxdb(record.getSystemMetadata().get(ACCOUNT_ID).trim()));
        }

        if(!StringUtils.isEmpty(record.getCollectionTarget())) {
            pointBuilder.tag("target",
                    escapeSpecialCharactersForInfluxdb(record.getCollectionTarget().trim()));
        }

        if(!StringUtils.isEmpty(record.getMonitoringSystem().toString())) {
            pointBuilder.tag("monitoringsystem",
                    escapeSpecialCharactersForInfluxdb(record.getMonitoringSystem().toString().trim()));
        }

        if(!StringUtils.isEmpty(record.getCollectionLabel())) {
            pointBuilder.tag("collectionlabel",
                    escapeSpecialCharactersForInfluxdb(record.getCollectionLabel().trim()));
        }

        addEntityTags(record, pointBuilder);
        addMonitoringZone(record, pointBuilder);
    }

    private void addMonitoringZone(Metric record, Point.Builder pointBuilder) {
        final String monitoringZone = record.getSystemMetadata().get(MONITORING_ZONE);
        if(!StringUtils.isEmpty(monitoringZone)){
            pointBuilder.tag("monitoringzone",
                    escapeSpecialCharactersForInfluxdb(monitoringZone.trim()));
        }
    }

    private void addEntityTags(Metric record, Point.Builder pointBuilder) {
        final String entityId = record.getSystemMetadata().get(ENTITY_ID);
        if(!StringUtils.isEmpty(entityId)) {
            pointBuilder.tag("entitysystemid",
                    escapeSpecialCharactersForInfluxdb(entityId.trim()));
        }
        if(!StringUtils.isEmpty(record.getDeviceLabel())) {
            pointBuilder.tag("devicelabel",
                    escapeSpecialCharactersForInfluxdb(record.getDeviceLabel().trim()));
        }
    }

    private String escapeSpecialCharactersForInfluxdb(String inputString){
        final String[] metaCharacters = {"\\","^","$","{","}","[","]","(",")",".","*","+","?","|","<",">","-","&","%"," "};

        for (int i = 0 ; i < metaCharacters.length ; i++){
            if(inputString.contains(metaCharacters[i])){
                inputString = inputString.replace(metaCharacters[i],"\\"+metaCharacters[i]);
            }
        }
        return inputString;
    }

    private String replaceSpecialCharacters(String inputString){
        final String[] metaCharacters = {"\\",":","^","$","{","}","[","]","(",")",".","*","+","?","|","<",">","-","&","%"," "};

        for (int i = 0 ; i < metaCharacters.length ; i++){
            if(inputString.contains(metaCharacters[i])){
                inputString = inputString.replace(metaCharacters[i],"_");
            }
        }
        return inputString;
    }

    private void populatePayload(final Metric record, final Point.Builder pointBuilder) {

        for(Map.Entry<String, Long> entry : record.getIvalues().entrySet()){
            String metricFieldName = replaceSpecialCharacters(entry.getKey());
            pointBuilder.tag(String.format("%s_unit", metricFieldName),
                    escapeSpecialCharactersForInfluxdb(record.getUnits().get(entry.getKey())));

            pointBuilder.addField(metricFieldName, entry.getValue().doubleValue());
        }

        for(Map.Entry<String, Double> entry : record.getFvalues().entrySet()){
            String metricFieldName = replaceSpecialCharacters(entry.getKey());
            pointBuilder.tag(String.format("%s_unit", metricFieldName),
                    escapeSpecialCharactersForInfluxdb(record.getUnits().get(entry.getKey())));

            pointBuilder.addField(metricFieldName, entry.getValue());
        }
    }

    private boolean isValid(String fieldName, CharSequence fieldValue, Metric record){
        if(StringUtils.isEmpty(fieldValue)){
            LOGGER.error("There is no value for the field '{}' in record [{}]", fieldName, record);
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
