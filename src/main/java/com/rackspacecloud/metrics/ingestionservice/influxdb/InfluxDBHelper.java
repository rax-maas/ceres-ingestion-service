package com.rackspacecloud.metrics.ingestionservice.influxdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.RouteProvider;
import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.TenantRoutes;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InfluxDBHelper {
    private Map<String, Map<String, InfluxDbInfoForRollupLevel>> influxDbInfoMap;
    private RestTemplate restTemplate;
    private RouteProvider routeProvider;


    private static final String INFLUXDB_INGEST_URL_FORMAT = "%s/write?db=%s&rp=%s&precision=s";
    private static final int MAX_TRY_FOR_INGEST = 5;

    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBHelper.class);

    @Autowired
    public InfluxDBHelper(RestTemplate restTemplate, RouteProvider routeProvider){
        this.restTemplate = restTemplate;
        this.routeProvider = routeProvider;
        this.influxDbInfoMap = new HashMap<>();
    }

    @Data
    @AllArgsConstructor
    class InfluxDbInfoForRollupLevel {
        private String path;
        private String databaseName;
        private String retentionPolicyName;
        private String retentionPolicy;
    }

    private Map<String, InfluxDbInfoForRollupLevel> getInfluxDbInfo(
            final String tenantId, final String measurement) throws Exception {

        String tenantIdAndMeasurementKey = String.format("%s:%s", tenantId, measurement);

        if(influxDbInfoMap.containsKey(tenantIdAndMeasurementKey))
            return influxDbInfoMap.get(tenantIdAndMeasurementKey);

        TenantRoutes tenantRoutes = getTenantRoutes(tenantId, measurement);

        Map<String, InfluxDbInfoForRollupLevel> influxDbInfoForTenant = new HashMap<>();

        for(Map.Entry<String, TenantRoutes.TenantRoute> entry : tenantRoutes.getRoutes().entrySet()) {
            String rollupLevel = entry.getKey();
            TenantRoutes.TenantRoute route = entry.getValue();
            String databaseName = route.getDatabaseName();
            String path = route.getPath();
            String retPolicyName = route.getRetentionPolicyName();
            String retPolicy = route.getRetentionPolicy();

            if(databaseExists(databaseName, path)) {
                // Check if retention policy exist
                if(retentionPolicyExists(retPolicyName, databaseName, path)) {
                    LOGGER.info("Database {} and retention policy {} already exist", databaseName, retPolicyName);
                    influxDbInfoForTenant.put(rollupLevel, new InfluxDbInfoForRollupLevel(
                            path, databaseName, retPolicyName, retPolicy
                    ));
                }
                else { // Create retention policy
                    boolean isDefault = false;
                    if(rollupLevel.equalsIgnoreCase("full")) isDefault = true;

                    if(createRetentionPolicy(databaseName, path, retPolicy, retPolicyName, isDefault)) {
                        influxDbInfoForTenant.put(rollupLevel, new InfluxDbInfoForRollupLevel(
                                path, databaseName, retPolicyName, retPolicy
                        ));
                    }
                    else {
                        LOGGER.error("Failed to create retention policy {} on database {}",
                                retPolicyName, databaseName);
                    }
                }
            }
            else {
                // InfluxDB create database API always returns 200 (http status code) for new or existing database
                // TODO: store already processed database so that we don't call createDatabase blindly
                if(createDatabase(databaseName, path, retPolicy, retPolicyName)) {
                    influxDbInfoForTenant.put(rollupLevel, new InfluxDbInfoForRollupLevel(
                            path, databaseName, retPolicyName, retPolicy
                    ));
                }
            }
        }

        influxDbInfoMap.put(tenantIdAndMeasurementKey, influxDbInfoForTenant);

        return influxDbInfoForTenant;
    }

    private TenantRoutes getTenantRoutes(String tenantId, String measurement) throws Exception {
        TenantRoutes tenantRoutes;

        try {
            tenantRoutes = routeProvider.getRoute(tenantId, measurement, restTemplate);
        }
        catch(Exception e) {
            String errMsg = String.format("Failed to get routes for tenantIdAndMeasurement [%s]", tenantId);
            LOGGER.error(errMsg, e);
            throw new Exception(errMsg, e);
        }

        if(tenantRoutes == null) throw new Exception("tenantRoutes is null.");
        return tenantRoutes;
    }

    private boolean databaseExists(final String databaseName, final String baseUrl) {
        String queryString = "q=SHOW DATABASES";
        ResponseEntity<String> response = getResponseEntity(baseUrl, queryString);

        try {
            if (response == null || response.getStatusCode() != HttpStatus.OK) {
                LOGGER.error("Response is null or Http Status code is not 'OK'");
                return false;
            }
            ObjectMapper mapper = new ObjectMapper();
            ShowDatabaseResult result = mapper.readValue(response.getBody(), ShowDatabaseResult.class);

            if(result != null
                    && result.results.length > 0
                    && result.results[0].series.length > 0
                    && result.results[0].series[0].values.length > 0
                    )
            {
                List<String> databases = new ArrayList<>();
                for(String[] strArray : result.results[0].series[0].getValues()) {
                    for(String database : strArray){
                        databases.add(database);
                    }
                }
                if(databases.contains(databaseName)) return true;
            }
        } catch (IOException e) {
            LOGGER.error("databaseExists failed with stack trace: {}", e);
        }

        return false;
    }

    private ResponseEntity<String> getResponseEntity(String baseUrl, String queryString) {
        ResponseEntity<String> response = null;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

        HttpEntity<String> entity = new HttpEntity<>(queryString, headers);

        String url = String.format("%s/query", baseUrl);
        try {
            response = restTemplate.exchange(url, HttpMethod.POST, entity, String.class);
        }
        catch(Exception ex){
            LOGGER.error("restTemplate.exchange threw exception with message: {}", ex.getMessage());
        }
        return response;
    }

    private boolean retentionPolicyExists(final String rp, final String databaseName, final String baseUrl) {
        String queryString = String.format("q=SHOW RETENTION POLICIES ON \"%s\"", databaseName);
        ResponseEntity<String> response = getResponseEntity(baseUrl, queryString);

        try {
            if (response == null || response.getStatusCode() != HttpStatus.OK) {
                LOGGER.error("Response is null or Http Status code is not 'OK'");
                return false;
            }

            ObjectMapper mapper = new ObjectMapper();
            ShowRetentionPoliciesResult result =
                    mapper.readValue(response.getBody(), ShowRetentionPoliciesResult.class);

            if(result != null
                    && result.results.length > 0
                    && result.results[0].series.length > 0
                    && result.results[0].series[0].values.length > 0
                    )
            {
                List<String> retPolicies = new ArrayList<>();
                for(String[] strings : result.results[0].series[0].values) {
                    retPolicies.add(strings[0]);
                }

                if(retPolicies.contains(rp)) return true;
            }
        }
        catch(IOException ex){
            LOGGER.error("Exception Message: {}; Failed with stack trace: {}", ex.getMessage(), ex);
        }

        return false;
    }

    private boolean createDatabase(final String databaseName, final String baseUrl,
                                   final String retPolicy, final String retPolicyName) {
        String queryString = String.format("q=CREATE DATABASE \"%s\" WITH DURATION %s NAME \"%s\"",
                databaseName, retPolicy, retPolicyName);
        ResponseEntity<String> response = getResponseEntity(baseUrl, queryString);

        if (responseCheck(response)) return true;

        return false;
    }

    private boolean createRetentionPolicy(final String databaseName, final String baseUrl,
            final String retPolicy, final String retPolicyName, boolean isDefault) {
        String queryString = String.format("q=CREATE RETENTION POLICY \"%s\" ON \"%s\" DURATION %s REPLICATION 1",
                retPolicyName, databaseName, retPolicy);

        if(isDefault) queryString = queryString + " DEFAULT";
        ResponseEntity<String> response = getResponseEntity(baseUrl, queryString);

        if (responseCheck(response)) return true;

        return false;
    }

    private boolean responseCheck(ResponseEntity<String> response) {
        try {
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

    public boolean ingestToInfluxDb(
            String payload, String tenantId, String measurement, String rollupLevel) throws Exception {
        // Get db and URL info to route data to
        Map<String, InfluxDbInfoForRollupLevel> influxDbInfoForTenant = getInfluxDbInfo(tenantId, measurement);
        InfluxDbInfoForRollupLevel influxDbInfoForRollupLevel = influxDbInfoForTenant.get(rollupLevel);

        if(influxDbInfoForRollupLevel == null) return false;

        String baseUrl = influxDbInfoForRollupLevel.getPath();
        String databaseName = influxDbInfoForRollupLevel.getDatabaseName();
        String retPolicyName = influxDbInfoForRollupLevel.getRetentionPolicyName();

        HttpHeaders headers = getHttpHeaders();
        HttpEntity<String> request = new HttpEntity<>(payload, headers);

        String url = String.format(INFLUXDB_INGEST_URL_FORMAT, baseUrl, databaseName, retPolicyName);

        if (writeToInfluxDb(payload, request, url)) return true;

        return false;
    }

    private boolean writeToInfluxDb(String payload, HttpEntity<String> request, String url) {
        ResponseEntity<String> response = null;
        int count = 0;

        while(response == null && count < MAX_TRY_FOR_INGEST) {
            try {
                response = restTemplate.exchange(url, HttpMethod.POST, request, String.class);
            }
            catch (Exception ex) {
                LOGGER.error("Using url [{}], Exception message: {}, trace: {}",
                        url, ex.getMessage(), ex);
            }

            if(response != null) {
                break;
            }
            else {
                count++;
            }
        }

        if(response == null){
            LOGGER.error("Using InfluxDB url [{}], got null in response for the payload -> {}", url, payload);
        }
        else if(response.getStatusCode() != HttpStatus.NO_CONTENT){
            LOGGER.error("Using InfluxDB url [{}], couldn't ingest the payload -> {}", url, payload);
        }
        else{
            return true;
        }
        return false;
    }

    private HttpHeaders getHttpHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        List<MediaType> mediaTypes = new ArrayList<>();
        mediaTypes.add(MediaType.TEXT_PLAIN);
        headers.setAccept(mediaTypes);

        return headers;
    }
}
