package com.rackspacecloud.metrics.ingestionservice;

import com.rackspacecloud.metrics.ingestionservice.influxdb.InfluxDBHelper;
import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.RouteProvider;
import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.TenantRoutes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles(value = {"raw-data-consumer","test"})
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {IngestionServiceApplicationTests.UNIFIED_METRICS_TOPIC})
public class InfluxDBHelperTests {

    static final String UNIFIED_METRICS_TOPIC = "unified.metrics.json";

    @Before
    public void setUp() {
    }

    @Test
    public void ingestToInfluxDb_withExistingDatabaseAndRetPolicy_shouldSucceed() throws Exception {
        RestTemplate restTemplateMock = mock(RestTemplate.class);
        RouteProvider routeProviderMock = mock(RouteProvider.class);
        InfluxDBHelper influxDBHelper = new InfluxDBHelper(restTemplateMock, routeProviderMock);
        String tenantId = "hybrid:1667601";
        String measurement = "cpu";
        String databaseName = "existing_db";
        String rpName = "existing_rp";

        doReturn(getTenantRoutes(tenantId, databaseName, rpName))
                .when(routeProviderMock).getRoute(tenantId, measurement, restTemplateMock);

        System.out.println(String.format("Testing with tenantIdAndMeasurement:[%s]; databaseName:[%s]; rpName:[%s]",
                tenantId, databaseName, rpName));

        successfulIngestionTest(influxDBHelper, restTemplateMock, tenantId, measurement, databaseName, rpName);
    }

    @Test
    public void ingestToInfluxDb_withNonExistingDatabase_shouldCreateDatabase() throws Exception {
        RestTemplate restTemplateMock = mock(RestTemplate.class);
        RouteProvider routeProviderMock = mock(RouteProvider.class);
        InfluxDBHelper influxDBHelper = new InfluxDBHelper(restTemplateMock, routeProviderMock);
        String tenantId = "hybrid:1667601";
        String measurement = "cpu";
        String databaseName = "existing_db";
        String rpName = "existing_rp";

        when(routeProviderMock.getRoute(anyString(), anyString(), any(RestTemplate.class)))
                .thenReturn(getTenantRoutes(tenantId, "non_existing_database", rpName));

        successfulIngestionTest(influxDBHelper, restTemplateMock, tenantId, measurement, databaseName, rpName);
    }

    @Test
    public void ingestToInfluxDb_withExistingDatabaseNonExistingRetentionPolicy_shouldCreateRetentionPolicy()
            throws Exception {
        RestTemplate restTemplateMock = mock(RestTemplate.class);
        RouteProvider routeProviderMock = mock(RouteProvider.class);
        InfluxDBHelper influxDBHelper = new InfluxDBHelper(restTemplateMock, routeProviderMock);
        String tenantId = "hybrid:1667601";
        String measurement = "cpu";
        String databaseName = "existing_db";
        String rpName = "existing_rp";

        /**
         * Here rpName is different from what routeProvider will provide. Goal is that these two values should
         * be different, so that it's creating a scenario where retention policy does not exist for a given database.
         */
        when(routeProviderMock.getRoute(anyString(), anyString(), any()))
                .thenReturn(getTenantRoutes(tenantId, databaseName, "non_existing_rp"));

        successfulIngestionTest(influxDBHelper, restTemplateMock, tenantId, measurement, databaseName, rpName);
    }

    private void successfulIngestionTest(
            InfluxDBHelper influxDBHelper, RestTemplate restTemplateMock,
            String tenantId, String measurement, String databaseName, String rpName) throws Exception {

        String payloadToIngestInInfluxDB = "valid payload";
        String rollupLevel = "full";
        String dbQueryString = "q=SHOW DATABASES";
        String rpQueryString = String.format("q=SHOW RETENTION POLICIES ON \"%s\"", databaseName);
        String createDbString =
                String.format("q=CREATE DATABASE \"non_existing_database\" WITH DURATION 5d NAME \"%s\"", rpName);

        String createRetPolicyString =
                String.format("q=CREATE RETENTION POLICY \"non_existing_rp\" " +
                        "ON \"%s\" DURATION 5d REPLICATION 1 DEFAULT", databaseName);

        when(restTemplateMock.exchange(anyString(), any(), any(), any(Class.class)))

                .thenAnswer((Answer<ResponseEntity<String>>) invocationOnMock -> {
                    String queryStringBody = ((HttpEntity)invocationOnMock.getArgument(2)).getBody().toString();

                    if(queryStringBody.equalsIgnoreCase(dbQueryString)) {
                        return getResultForExistingDatabases(databaseName);
                    }
                    else if(queryStringBody.equalsIgnoreCase(rpQueryString)) {
                        return getResultForExistingRetentionPolicies(rpName);
                    }
                    else if(queryStringBody.equalsIgnoreCase(payloadToIngestInInfluxDB)) {
                        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
                    }
                    else if(queryStringBody.equalsIgnoreCase(createDbString)) {
                        return new ResponseEntity<>(HttpStatus.OK);
                    }
                    else if(queryStringBody.equalsIgnoreCase(createRetPolicyString)) {
                        return new ResponseEntity<>(HttpStatus.OK);
                    }
                    else {
                        return null;
                    }
                });

        Assert.assertTrue("Ingest to influxDB failed in test.",
                influxDBHelper.ingestToInfluxDb(payloadToIngestInInfluxDB, tenantId, measurement, rollupLevel));
    }

    private TenantRoutes getTenantRoutes(String tenantId, String databaseName, String rpName) {
        TenantRoutes tenantRoutes = new TenantRoutes();
        tenantRoutes.setTenantIdAndMeasurement(tenantId);
        Map<String, TenantRoutes.TenantRoute> routes = tenantRoutes.getRoutes();

        routes.put("full", new TenantRoutes.TenantRoute(
                "valid_url",
                databaseName,
                rpName,
                "5d",
                10000
        ));

        return tenantRoutes;
    }

    private ResponseEntity<String> getResultForExistingDatabases(String databaseName) {
        String responseBody = String.format(
                "{\"results\":[{\"series\":" +
                        "[{\"name\":\"databases\",\"tags\":null,\"columns\":[\"name\"],\"values\":[[\"_internal\"]," +
                        "[\"%s\"]]}],\"error\":null}],\"error\":null}", databaseName);
        ResponseEntity<String> response = new ResponseEntity<>(responseBody, HttpStatus.OK);

        return response;
    }

    private ResponseEntity<String> getResultForExistingRetentionPolicies(String rpName) {
        String responseBody = String.format(
                "{\"results\":[{\"series\":[{\"name\":null,\"tags\":null,\"columns\":" +
                        "[\"name\",\"duration\",\"shardGroupDuration\",\"replicaN\",\"default\"]," +
                        "\"values\":[[\"%s\",\"120h0m0s\",\"24h0m0s\",1.0,true]]}]," +
                        "\"error\":null}],\"error\":null}", rpName);
        ResponseEntity<String> response = new ResponseEntity<>(responseBody, HttpStatus.OK);

        return response;
    }
}
