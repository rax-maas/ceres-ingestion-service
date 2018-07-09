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
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    @Value("${influxdb.url}")
    private String influxdbUrl;

    private static final Logger LOGGER = LoggerFactory.getLogger(UnifiedMetricsListener.class);

    @Autowired
    public UnifiedMetricsListener(IAuthTokenProvider authTokenProvider, RestTemplate restTemplate){
        this.authTokenProvider = authTokenProvider;
        this.restTemplate = restTemplate;
    }

    @Data
    class MetricNameItems {
        String entityId;
        String checkType;
        String checkId;
    }

    /**
     * This listener listens to unified_metrics.json topic.
     * @param record
     */
    @KafkaListener(topics = "#{'${kafka.topics.in}'.split(',')}")
    public boolean listen(Metrics record){
        LOGGER.debug("Received record:{}", record);

        boolean isInfluxdbIngestionSuccessful = false;

        try {
            String payload = convertToInfluxdbIngestFormat(record);

            if (payload != null) {
                isInfluxdbIngestionSuccessful = ingestToInfluxdb(payload);
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
    private boolean ingestToInfluxdb(String payload) throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        List<MediaType> mediaTypes = new ArrayList<>();
        mediaTypes.add(MediaType.TEXT_PLAIN);
        headers.setAccept(mediaTypes);

//        headers.set(X_AUTH_TOKEN, authTokenProvider.getAuthToken());

        HttpEntity<String> request = new HttpEntity<>(payload, headers);
        String dbName = String.format("tenant_id_%s", authTokenProvider.getTenantId());
        String url = String.format(INFLUXDB_INGEST_URL_FORMAT, influxdbUrl, dbName);

        ResponseEntity<String> response = null;
        int count = 0;

        while(response == null && count < MAX_TRY_FOR_INGEST) {
            try {
                response = restTemplate.exchange(url, HttpMethod.POST, request, String.class);
            } catch (Exception ex) {
                LOGGER.error("Using url [{}], Exception message: {}", url, ex.getMessage());
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
        if(!isValid(TENANT_ID, record.getTenantId(), record)){
            return null;
        }

        if(!isValid(TIMESTAMP, record.getTimestamp(), record)){
            return null;
        }

        if(record.getCheck() == null) return null;

        String measurementName = record.getCheck().getType().toString().replace('.','_');

        Set<String> tagSet = getTagSet(record);

        Instant instant = Instant.parse(record.getTimestamp());

        // Convert into nano seconds
        long collectionTime = instant.getEpochSecond()*1000*1000*1000 + instant.getNano(); //instant.toEpochMilli() + instant.getNano();

        String payload = createPayload(record, collectionTime, tagSet, measurementName);
        if (payload == null) {
            LOGGER.error("There is no payload for influxdb. Record:[{}]", record);
            return null;
        }

        return payload;
    }

    private Set<String> getTagSet(Metrics record) {
        Set<String> tagSet = new HashSet<>();

        String tenantId = record.getTenantId().toString();
        tagSet.add(String.format("tenantid=%s", escapeSpecialCharactersForInfluxdb(tenantId.trim())));

        if(!StringUtils.isEmpty(record.getSystemAccountId()))
            tagSet.add(String.format("systemaccountid=%s", escapeSpecialCharactersForInfluxdb(record.getSystemAccountId().toString().trim())));

        if(!StringUtils.isEmpty(record.getTarget().toString()))
            tagSet.add(String.format("target=%s", escapeSpecialCharactersForInfluxdb(record.getTarget().toString().trim())));

        if(!StringUtils.isEmpty(record.getMonitoringSystem().toString()))
            tagSet.add(String.format("monitoringsystem=%s", escapeSpecialCharactersForInfluxdb(record.getMonitoringSystem().toString().trim())));

        addCheck(record.getCheck(), tagSet);
        addEntityTags(record, tagSet);
        addMonitoringZone(record, tagSet);
        return tagSet;
    }

    private void addCheck(Check check, Set<String> tagSet) {
        if(!StringUtils.isEmpty(check.getSystemId().toString()))
            tagSet.add(String.format("checksystemid=%s", escapeSpecialCharactersForInfluxdb(check.getSystemId().toString().trim())));

        if(!StringUtils.isEmpty(check.getLabel().toString()))
            tagSet.add(String.format("checklabel=%s", escapeSpecialCharactersForInfluxdb(check.getLabel().toString().trim())));
    }

    private void addMonitoringZone(Metrics record, Set<String> tagSet) {
        if(record.getMonitoringZone() != null){
            MonitoringZone monitoringZone = record.getMonitoringZone();
            if(!StringUtils.isEmpty(monitoringZone.getSystemId().toString()))
                tagSet.add(String.format("monitoringzonesystemid=%s", escapeSpecialCharactersForInfluxdb(monitoringZone.getSystemId().toString().trim())));

            if(!StringUtils.isEmpty(monitoringZone.getLabel().toString()))
                tagSet.add(String.format("monitoringzonelabel=%s", escapeSpecialCharactersForInfluxdb(monitoringZone.getLabel().toString().trim())));
        }
    }

    private void addEntityTags(Metrics record, Set<String> tagSet) {
        if(record.getEntity() != null){
            Entity entity = record.getEntity();
            if(!StringUtils.isEmpty(entity.getSystemId().toString()))
                tagSet.add(String.format("entitysystemid=%s", escapeSpecialCharactersForInfluxdb(entity.getSystemId().toString().trim())));

            if(!StringUtils.isEmpty(entity.getLabel().toString()))
                tagSet.add(String.format("entitylabel=%s", escapeSpecialCharactersForInfluxdb(entity.getLabel().toString().trim())));
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

    private String createPayload(
            final Metrics record, final long collectionTime, final Set<String> tagSet, final String measurementName) {

        List<String> payload = new ArrayList<>();

        for(MetricValue value : record.getValues()){
            Set<String> tempTagSet = new HashSet<>(tagSet);
            try {
                tempTagSet.add(String.format("name=%s", escapeSpecialCharactersForInfluxdb(value.getName().toString().trim())));

                if(!StringUtils.isEmpty(value.getUnits()))
                    tempTagSet.add(String.format("units=%s", escapeSpecialCharactersForInfluxdb(value.getUnits().toString().trim())));

                double val = Double.parseDouble(value.getValue().toString());
                String fieldSet = String.format("metricvalue=%s", val);

                String tagSetString = String.join(",", tempTagSet);

                String payloadItem =
                        String.format("%s,%s %s %s", measurementName, tagSetString, fieldSet, collectionTime);
//                        String.format("%s,%s %s", measurementName, tagSetString, fieldSet);

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
