package com.rackspacecloud.metrics.kafkainfluxdbconsumer;

import com.rackspace.maas.model.*;
import com.rackspacecloud.metrics.kafkainfluxdbconsumer.provider.IAuthTokenProvider;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.*;

@Component
public class UnifiedMetricsListener {

  IAuthTokenProvider authTokenProvider;
    RestTemplate restTemplate;

    private long messageProcessedCount = 0;

    // At the end of every 1000 messages, log this information
    private static final int MESSAGE_PROCESS_REPORT_COUNT = 1000;

    private static final String TIMESTAMP = "timestamp";
    private static final String COLLECTION_TIME = "collectionTime";
    private static final String TENANT_ID = "tenantId";
    private static final String CHECK_TYPE = "checkType";
    private static final String ACCOUNT_ID = "accountId";
    private static final String MONITORING_ZONE = "monitoringZone";
    private static final String ENTITY_ID = "entityId";
    private static final String TTL = "ttlInSeconds";
    private static final String X_AUTH_TOKEN = "x-auth-token";
    private static final String INFLUXDB_INGEST_URL_FORMAT = "%s/write?db=%s&rp=rp_3d";
    private static final int TTL_IN_SECONDS = 172800;
    private static final int MAX_TRY_FOR_INGEST = 5;
    private static Map<String, InfluxdbInfo> influxDbInfoMap;

    // TODO: Delete it once test is done
    // private static final int MAX_DATABASE_COUNT_FOR_TEST = 50;
    private int databaseCountForTest = 0;

    @Value("${influxdb.url}")
    private String influxdbUrl;

    private static final Logger LOGGER = LoggerFactory.getLogger(UnifiedMetricsListener.class);

    @Data
    static class InfluxdbInfo {
        private String baseUrlInTheCluster;
        private String databaseName;

        public InfluxdbInfo(String baseUrlInTheCluster, String databaseName){
            this.baseUrlInTheCluster = baseUrlInTheCluster;
            this.databaseName = databaseName;
        }
    }

    @Data
    static class TagAndFieldsSets {
        private Set<String> tagSet;
        private Set<String> fieldSet;

        public TagAndFieldsSets(){
            tagSet = new HashSet<>();
            fieldSet = new HashSet<>();
        }
    }

    @Autowired
    public UnifiedMetricsListener(IAuthTokenProvider authTokenProvider, RestTemplate restTemplate){
        this.authTokenProvider = authTokenProvider;
        this.restTemplate = restTemplate;
        influxDbInfoMap = new HashMap<>();
    }

    /**
     * This listener listens to maas_metrics.json topic.
     * @param record
     */
    @KafkaListener(topicPartitions = @TopicPartition(topic = "${kafka.topics.in}",
            partitionOffsets = {
                    @PartitionOffset(
                            partition = "${kafka.topics.partition}",
                            initialOffset = "${kafka.topics.offset}"
                    )}))
    public boolean listen(
            @Payload final Metric record,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partitionId,
            @Header(KafkaHeaders.OFFSET) final long offset,
            final Acknowledgment ack){
        LOGGER.debug("Received record:{}", record);

        boolean isInfluxdbIngestionSuccessful = false;

        final String tenantId = record.getSystemMetadata().get(TENANT_ID);
        if(!isValid(TENANT_ID, tenantId, record)) {
            LOGGER.error("Invalid tenant ID [{}] in the received record [{}]", tenantId, record);
            return false;
        }

//        // TODO: DELETE IT AFTER TEST
//        tenantId = getNextTenantId();

        // Get db and URL info to route data to
        InfluxdbInfo influxdbInfo = getInfluxdbInfo(tenantId);

        if(StringUtils.isEmpty(influxdbInfo.databaseName)) return false;

        try {
            String payload = convertToInfluxdbIngestFormat(record);

            if (payload != null) {
                isInfluxdbIngestionSuccessful = ingestToInfluxdb(payload, influxdbInfo);

                if(isInfluxdbIngestionSuccessful) {
                    ack.acknowledge();
                    LOGGER.info("Successfully processed partionId:{}, offset:{} at {}", partitionId, offset, Instant.now());
                }
                else {
                    // TODO: retry? OR write messages into some 'maas_metrics_error' topic, so that later on
                    // we can read it from that error topic
                }
            }
        }
        catch(Exception e){
            LOGGER.error("listen method failed with exception message [{}]", e.getMessage());
        }

        LOGGER.debug("Done processing for record:{}", record);

        // Reset the counter
        if(messageProcessedCount == Long.MAX_VALUE) messageProcessedCount = 0;
        messageProcessedCount++;

        if(messageProcessedCount % MESSAGE_PROCESS_REPORT_COUNT == 0) {
            LOGGER.info("Processed {} messages so far after start or reset...", getMessageProcessedCount());
        }

        return isInfluxdbIngestionSuccessful;
    }

    private boolean createDatabase(final InfluxdbInfo influxdbInfo) {
        String dbCreatePayload = String.format("q=CREATE DATABASE \"%s\" WITH DURATION %s NAME \"%s\"",
                influxdbInfo.databaseName, "5d", "rp_5d");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        HttpEntity<String> entity = new HttpEntity<>(dbCreatePayload, headers);

        String url = String.format("%s/query", influxdbInfo.baseUrlInTheCluster);
        try {
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, entity, String.class);

            if (response == null || response.getStatusCode() != HttpStatus.OK) {
                LOGGER.error("Response is null or Http Status code is not 'OK'");
            }
            //TODO: why return true? Find out what status code we get when database creation fails or succeeds.
            return true;
        }
        catch(Exception ex){
            LOGGER.error("restTemplate.exchange threw exception with message: {}", ex.getMessage());
        }

        return false;
    }

    private InfluxdbInfo getInfluxdbInfo(String tenantId) {
        if(influxDbInfoMap.containsKey(tenantId)) return influxDbInfoMap.get(tenantId);
        int port = getRoute(tenantId);

        String baseUrl = String.format("%s:%d", influxdbUrl, port);
//        String baseUrl = "http://localhost:8086";

        String cleanedTenantId = replaceSpecialCharacters(tenantId);
        String databaseName = "db_" + cleanedTenantId;

        InfluxdbInfo influxDbInfo = new InfluxdbInfo(baseUrl, databaseName);
        influxDbInfoMap.put(tenantId, influxDbInfo);
        return influxDbInfo;
    }

    private int getRoute(String tenantId) {
//        // TODO: Work on routing given tenant to specific Influxdb instance
//        return (databaseCountForTest < (MAX_DATABASE_COUNT_FOR_TEST / 2)) ? 81 : 80;

        // TODO: Temporary routing solution.
        return ((tenantId.hashCode())%2 == 0) ? 80 : 81;
    }

//    // TODO: JUST FOR TESTING NUMBER OF DATABASES PER INSTANCE
//    private String getNextTenantId(){
//        if(databaseCountForTest == MAX_DATABASE_COUNT_FOR_TEST) databaseCountForTest = 0;
//        return "" + databaseCountForTest++;
//    }

    /**
     * Get the current count of the total message processed by the consumer
     * @return
     */
    public long getMessageProcessedCount(){
        return messageProcessedCount;
    }

    /**
     * This method is responsible for ingesting data into Influxdb using ingest API.
     * @param payload
     */
    private boolean ingestToInfluxdb(String payload, InfluxdbInfo influxdbInfo) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        List<MediaType> mediaTypes = new ArrayList<>();
        mediaTypes.add(MediaType.TEXT_PLAIN);
        headers.setAccept(mediaTypes);

//        headers.set(X_AUTH_TOKEN, authTokenProvider.getAuthToken());

        HttpEntity<String> request = new HttpEntity<>(payload, headers);

        String url = String.format(INFLUXDB_INGEST_URL_FORMAT,
                influxdbInfo.baseUrlInTheCluster, influxdbInfo.databaseName);

        ResponseEntity<String> response = null;
        int count = 0;

        // TODO: Simplify this logic a little to make it maintainable
        while((response == null || response.getStatusCode() != HttpStatus.NO_CONTENT)
                && count < MAX_TRY_FOR_INGEST) {
            try {
                response = restTemplate.exchange(url, HttpMethod.POST, request, String.class);

                if (response.getStatusCode() == HttpStatus.NOT_FOUND) {
                    LOGGER.info("Database {} is not found in route [{}].",
                            influxdbInfo.databaseName, influxdbInfo.baseUrlInTheCluster);
                    createDatabase(influxdbInfo);
                }
            } catch (Exception ex) {
                LOGGER.error("Using url [{}], Exception message: {}", url, ex.getMessage());

                if(((HttpClientErrorException) ex).getStatusCode().value() == 404){
                    LOGGER.info("Database {} is not found in route [{}].",
                            influxdbInfo.databaseName, influxdbInfo.baseUrlInTheCluster);
                    createDatabase(influxdbInfo);
                }
            }
            if(response != null) {
                break;
            }
            else {
                count++;
            }
        }

        if(response == null){
            LOGGER.error("Using Influxdb url [{}], got null in response for the payload -> {}", url, payload);
        }
        else if(response.getStatusCode() != HttpStatus.NO_CONTENT){
            LOGGER.error("Using Influxdb url [{}], couldn't ingest the payload -> {}", url, payload);
        }
        else{
            if(messageProcessedCount % MESSAGE_PROCESS_REPORT_COUNT == 0) {
                LOGGER.info("Processed {} messages. Successfully ingested current payload in Influxdb -> {}",
                        messageProcessedCount, payload);
            }
            return true;
        }

        return false;
    }

    /**
     * Convert the received record into Influxdb ingest input format
     * @param record
     * @return
     */
    private String convertToInfluxdbIngestFormat(Metric record){

        if(!isValid(TIMESTAMP, record.getTimestamp(), record)) return null;

        if(record.getSystemMetadata().get(CHECK_TYPE) == null) return null;

        TagAndFieldsSets tagAndFieldsSets = getTagAndFieldsSets(record);
        if (tagAndFieldsSets == null) return null;

        Instant instant = Instant.parse(record.getTimestamp());

        // Convert into nano seconds
        long collectionTime = instant.getEpochSecond()*1000*1000*1000 + instant.getNano();

        String payload = createPayload(record, collectionTime, tagAndFieldsSets);
        if (payload == null) {
            LOGGER.error("There is no payload for influxdb. Record:[{}]", record);
            return null;
        }

        return payload;
    }

    private TagAndFieldsSets getTagAndFieldsSets(Metric record) {
        TagAndFieldsSets tagAndFieldsSets = new TagAndFieldsSets();

        Set<String> tagSet = tagAndFieldsSets.tagSet;

        if(!StringUtils.isEmpty(record.getSystemMetadata().get(ACCOUNT_ID)))
            tagSet.add(String.format("systemaccountid=\"%s\"",
                    escapeSpecialCharactersForInfluxdb(record.getSystemMetadata().get(ACCOUNT_ID).trim())));

        if(!StringUtils.isEmpty(record.getCollectionTarget()))
            tagSet.add(String.format("target=%s",
                    escapeSpecialCharactersForInfluxdb(record.getCollectionTarget().trim())));

        if(!StringUtils.isEmpty(record.getMonitoringSystem().toString()))
            tagSet.add(String.format("monitoringsystem=%s",
                    escapeSpecialCharactersForInfluxdb(record.getMonitoringSystem().toString().trim())));

        addCheck(record, tagAndFieldsSets);
        addEntityTags(record, tagAndFieldsSets);
        addMonitoringZone(record, tagAndFieldsSets);

        return tagAndFieldsSets;
    }

    private void addCheck(Metric check, TagAndFieldsSets tagAndFieldsSets) {
        if(!StringUtils.isEmpty(check.getCollectionLabel()))
            tagAndFieldsSets.tagSet.add(String.format("collectionlabel=%s",
                    escapeSpecialCharactersForInfluxdb(check.getCollectionLabel().trim())));
    }

    private void addMonitoringZone(Metric record, TagAndFieldsSets tagAndFieldsSets) {
      final String monitoringZone = record.getSystemMetadata().get(MONITORING_ZONE);
      if(!StringUtils.isEmpty(monitoringZone)){
                tagAndFieldsSets.tagSet.add(String.format("monitoringzone=\"%s\"",
                        escapeSpecialCharactersForInfluxdb(
                            monitoringZone.trim())));
        }
    }

    private void addEntityTags(Metric record, TagAndFieldsSets tagAndFieldsSets) {
        final String entityId = record.getSystemMetadata().get(ENTITY_ID);
        if(!StringUtils.isEmpty(entityId))
            tagAndFieldsSets.tagSet.add(String.format("entitysystemid=\"%s\"",
                    escapeSpecialCharactersForInfluxdb(entityId.trim())));

        if(!StringUtils.isEmpty(record.getDeviceLabel()))
            tagAndFieldsSets.tagSet.add(String.format("devicelabel=%s",
                    escapeSpecialCharactersForInfluxdb(record.getDeviceLabel().trim())));
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

    private String createPayload(
        final Metric record, final long collectionTime,
        final TagAndFieldsSets tagAndFieldsSets) {

        List<String> payload = new ArrayList<>();

        for(Map.Entry<String, Long> entry : record.getIvalues().entrySet()){
            addValueTo(payload, collectionTime, tagAndFieldsSets,
                entry.getKey(),
                entry.getValue().doubleValue(),
                record.getUnits().get(entry.getKey()));
        }
        for(Map.Entry<String, Double> entry : record.getFvalues().entrySet()){
            addValueTo(payload, collectionTime, tagAndFieldsSets,
                entry.getKey(),
                entry.getValue(),
                record.getUnits().get(entry.getKey()));
        }

        if(payload.size() == 0) return null;

        return String.join("\n",payload);
    }

    private void addValueTo(List<String> payload, long collectionTime,
                            TagAndFieldsSets tagAndFieldsSets,
                            String metricName, double value, String units) {

        Set<String> tempTagSet = new HashSet<>(tagAndFieldsSets.tagSet);
        Set<String> tempFieldSet = new HashSet<>(tagAndFieldsSets.fieldSet);

        try {
            if(!StringUtils.isEmpty(units))
                tempTagSet.add(String.format("units=%s", escapeSpecialCharactersForInfluxdb(units)));

            tempFieldSet.add(String.format("metricvalue=%f", value));

            String tagSetString = String.join(",", tempTagSet);
            String fieldSetString = String.join(",", tempFieldSet);

            // payload item's string format is to create the line protocol. So, spaces and comma are there for a reason.
            String payloadItem = String.format("%s,%s %s %s",
                    replaceSpecialCharacters(metricName), tagSetString, fieldSetString, collectionTime);

            payload.add(payloadItem);
        } catch(NumberFormatException e){
            // Skip the metric item as there is no value to put.
        }
    }

    private boolean isValid(String fieldName, CharSequence fieldValue, Metric record){
        if(StringUtils.isEmpty(fieldValue)){
            LOGGER.error("There is no value for the field '{}' in record [{}]", fieldName, record);
            return false;
        }

        return true;
    }
}
