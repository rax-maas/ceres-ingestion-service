package com.rackspacecloud.metrics.ingestionservice.influxdb;

import com.rackspacecloud.metrics.ingestionservice.models.TenantRoute;
import com.rackspacecloud.metrics.ingestionservice.providers.IRouteProvider;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import java.util.*;

public class InfluxDBHelper {
    private Map<String, Set<String>> urlDatabasesMap;
    private Map<String, InfluxdbInfo> influxDbInfoMap;
    private RestTemplate restTemplate;
    private IRouteProvider routeProvider;


    private static final String INFLUXDB_INGEST_URL_FORMAT = "%s/write?db=%s&rp=%s&precision=s";
    private static final int MAX_TRY_FOR_INGEST = 5;

    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBHelper.class);

    @Autowired
    public InfluxDBHelper(RestTemplate restTemplate, IRouteProvider routeProvider){
        this.restTemplate = restTemplate;
        this.routeProvider = routeProvider;
        this.urlDatabasesMap = new HashMap<>();
        this.influxDbInfoMap = new HashMap<>();
    }

    @Data
    class InfluxdbInfo {
        private String baseUrlInTheCluster;
        private String databaseName;

        public InfluxdbInfo(String baseUrlInTheCluster, String databaseName){
            this.baseUrlInTheCluster = baseUrlInTheCluster;
            this.databaseName = databaseName;
        }
    }

    private InfluxdbInfo getInfluxdbInfo(final String tenantId) {
        if(influxDbInfoMap.containsKey(tenantId)) return influxDbInfoMap.get(tenantId);

        TenantRoute tenantRoute = routeProvider.getRoute(tenantId, restTemplate);

        if(StringUtils.isEmpty(tenantRoute.getPath())){
            LOGGER.error("TenantRoute's path '{}' is empty", tenantRoute.getPath());
            return null;
        }

        if(StringUtils.isEmpty(tenantRoute.getDatabaseName())){
            LOGGER.error("TenantRoute's database name '{}' is empty", tenantRoute.getDatabaseName());
            return null;
        }

        InfluxdbInfo influxDbInfo = new InfluxdbInfo(tenantRoute.getPath(), tenantRoute.getDatabaseName());
        influxDbInfoMap.put(tenantId, influxDbInfo);
        return influxDbInfo;
    }

    private boolean createDatabase(
            final String databaseName,
            final String baseUrl,
            final String retPolicy,
            final String retPolicyName) {

        String dbCreatePayload = String.format("q=CREATE DATABASE \"%s\" WITH DURATION %s NAME \"%s\"",
                databaseName, retPolicy, retPolicyName);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        HttpEntity<String> entity = new HttpEntity<>(dbCreatePayload, headers);

        String url = String.format("%s/query", baseUrl);
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

    public boolean ingestToInfluxdb(String payload, String tenantId, String retPolicy, String retPolicyName) {

        // Get db and URL info to route data to
        InfluxdbInfo influxdbInfo = getInfluxdbInfo(tenantId);

        if(influxdbInfo == null) return false;

        HttpHeaders headers = getHttpHeaders();
        HttpEntity<String> request = new HttpEntity<>(payload, headers);

        String url = String.format(INFLUXDB_INGEST_URL_FORMAT,
                influxdbInfo.baseUrlInTheCluster, influxdbInfo.databaseName, retPolicyName);

        ResponseEntity<String> response = null;
        int count = 0;

        while(response == null && count < MAX_TRY_FOR_INGEST) {
            try {
                // InfluxDB create database API always returns 200 (http status code) for new or existing database
                // TODO: store already processed database so that we don't call createDatabase blindly
                createDatabase(influxdbInfo.databaseName,
                        influxdbInfo.baseUrlInTheCluster, retPolicy, retPolicyName);
                response = restTemplate.exchange(url, HttpMethod.POST, request, String.class);
            }
            catch (Exception ex) {
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
