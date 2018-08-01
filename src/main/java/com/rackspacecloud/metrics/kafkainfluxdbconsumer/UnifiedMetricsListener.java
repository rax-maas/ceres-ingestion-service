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
    private static final String METRIC_VALUE = "metricValue";
    private static final String METRIC_NAME = "metricName";
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
     * This listener listens to unified_metrics.json topic.
     * @param record
     */
    @KafkaListener(topics = "#{'${kafka.topics.in}'.split(',')}")
    public boolean listen(Metrics record){
        LOGGER.debug("Received record:{}", record);

        boolean isInfluxdbIngestionSuccessful = false;

        if(!isValid(TENANT_ID, record.getTenantId(), record)) {
            LOGGER.error("Invalid tenant ID [{}] in the received record [{}]", record.getTenantId(), record);
            return false;
        }

        String tenantId = record.getTenantId().toString();

//        // TODO: DELETE IT AFTER TEST
//        tenantId = getNextTenantId();

        // Get db and URL info to route data to
        InfluxdbInfo influxdbInfo = getInfluxdbInfo(tenantId);

        if(StringUtils.isEmpty(influxdbInfo.databaseName)) return false;

        try {
            String payload = convertToInfluxdbIngestFormat(record);

            if (payload != null) {
                isInfluxdbIngestionSuccessful = ingestToInfluxdb(payload, influxdbInfo);
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
                influxdbInfo.databaseName, "3d", "rp_3d");

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
    private String convertToInfluxdbIngestFormat(Metrics record){

        if(!isValid(TIMESTAMP, record.getTimestamp(), record)) return null;

        if(record.getCheck() == null) return null;

        String measurementName = record.getCheck().getType().toString().replace('.','_');

        TagAndFieldsSets tagAndFieldsSets = getTagAndFieldsSets(record);
        if (tagAndFieldsSets == null) return null;

        Instant instant = Instant.parse(record.getTimestamp());

        // Convert into nano seconds
        long collectionTime = instant.getEpochSecond()*1000*1000*1000 + instant.getNano();

        String payload = createPayload(record, collectionTime, tagAndFieldsSets, measurementName);
        if (payload == null) {
            LOGGER.error("There is no payload for influxdb. Record:[{}]", record);
            return null;
        }

        return payload;
    }

    private TagAndFieldsSets getTagAndFieldsSets(Metrics record) {
        TagAndFieldsSets tagAndFieldsSets = new TagAndFieldsSets();

        Set<String> tagSet = tagAndFieldsSets.tagSet;

        if(!StringUtils.isEmpty(record.getSystemAccountId()))
            tagSet.add(String.format("systemaccountid=\"%s\"",
                    escapeSpecialCharactersForInfluxdb(record.getSystemAccountId().toString().trim())));

        if(!StringUtils.isEmpty(record.getTarget().toString()))
            tagSet.add(String.format("target=%s",
                    escapeSpecialCharactersForInfluxdb(record.getTarget().toString().trim())));

        if(!StringUtils.isEmpty(record.getMonitoringSystem().toString()))
            tagSet.add(String.format("monitoringsystem=%s",
                    escapeSpecialCharactersForInfluxdb(record.getMonitoringSystem().toString().trim())));

        addCheck(record.getCheck(), tagAndFieldsSets);
        addEntityTags(record, tagAndFieldsSets);
        addMonitoringZone(record, tagAndFieldsSets);

        return tagAndFieldsSets;
    }

    private void addCheck(Check check, TagAndFieldsSets tagAndFieldsSets) {
        if(!StringUtils.isEmpty(check.getSystemId().toString()))
            tagAndFieldsSets.tagSet.add(String.format("checksystemid=\"%s\"",
                    escapeSpecialCharactersForInfluxdb(check.getSystemId().toString().trim())));

        if(!StringUtils.isEmpty(check.getLabel().toString()))
            tagAndFieldsSets.tagSet.add(String.format("checklabel=%s",
                    escapeSpecialCharactersForInfluxdb(check.getLabel().toString().trim())));
    }

    private void addMonitoringZone(Metrics record, TagAndFieldsSets tagAndFieldsSets) {
        if(record.getMonitoringZone() != null){
            MonitoringZone monitoringZone = record.getMonitoringZone();
            if(!StringUtils.isEmpty(monitoringZone.getSystemId().toString()))
                tagAndFieldsSets.tagSet.add(String.format("monitoringzonesystemid=\"%s\"",
                        escapeSpecialCharactersForInfluxdb(monitoringZone.getSystemId().toString().trim())));

            if(!StringUtils.isEmpty(monitoringZone.getLabel().toString()))
                tagAndFieldsSets.tagSet.add(String.format("monitoringzonelabel=%s",
                        escapeSpecialCharactersForInfluxdb(monitoringZone.getLabel().toString().trim())));
        }
    }

    private void addEntityTags(Metrics record, TagAndFieldsSets tagAndFieldsSets) {
        if(record.getEntity() != null){
            Entity entity = record.getEntity();
            if(!StringUtils.isEmpty(entity.getSystemId().toString()))
                tagAndFieldsSets.tagSet.add(String.format("entitysystemid=\"%s\"",
                        escapeSpecialCharactersForInfluxdb(entity.getSystemId().toString().trim())));

            if(!StringUtils.isEmpty(entity.getLabel().toString()))
                tagAndFieldsSets.tagSet.add(String.format("entitylabel=%s",
                        escapeSpecialCharactersForInfluxdb(entity.getLabel().toString().trim())));
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

    private String createPayload(
            final Metrics record, final long collectionTime,
            final TagAndFieldsSets tagAndFieldsSets, final String measurementName) {

        List<String> payload = new ArrayList<>();

        for(MetricValue value : record.getValues()){
            Set<String> tempTagSet = new HashSet<>(tagAndFieldsSets.tagSet);
            Set<String> tempFieldSet = new HashSet<>(tagAndFieldsSets.fieldSet);

            try {
                tempTagSet.add(String.format("name=%s",
                        escapeSpecialCharactersForInfluxdb(value.getName().toString().trim())));

                if(!StringUtils.isEmpty(value.getUnits()))
                    tempTagSet.add(String.format("units=%s",
                            escapeSpecialCharactersForInfluxdb(value.getUnits().toString().trim())));

                double val = Double.parseDouble(value.getValue().toString());
                tempFieldSet.add(String.format("metricvalue=%s", val));

                String tagSetString = String.join(",", tempTagSet);
                String fieldSetString = String.join(",", tempFieldSet);

                // payload item's string format is to create the line protocol. So, spaces and comma are there for a reason.
                String payloadItem = String.format("%s,%s %s %s", measurementName, tagSetString, fieldSetString, collectionTime);

                payload.add(payloadItem);
            } catch(NumberFormatException e){
                // Skip the metric item as there is no value to put.
            }
        }

        if(payload.size() == 0) return null;

        return String.join("\n",payload);
    }

    private boolean isValid(String fieldName, CharSequence fieldValue, Metrics record){
        if(StringUtils.isEmpty(fieldValue)){
            LOGGER.error("There is no value for the field '{}' in record [{}]", fieldName, record);
            return false;
        }

        return true;
    }
}
