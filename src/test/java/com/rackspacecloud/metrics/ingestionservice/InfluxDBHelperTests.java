package com.rackspacecloud.metrics.ingestionservice;

import com.rackspacecloud.metrics.ingestionservice.influxdb.InfluxDBHelper;
import com.rackspacecloud.metrics.ingestionservice.influxdb.GCLineProtocolBackupService;
import com.rackspacecloud.metrics.ingestionservice.influxdb.LocalUUID;
import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.RouteProvider;
import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.TenantRoutes;
import com.rackspacecloud.metrics.ingestionservice.utils.InfluxDBFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.stubbing.OngoingStubbing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.*;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles(value = {"raw-data-consumer","test"})
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {IngestionServiceApplicationTests.UNIFIED_METRICS_TOPIC})
public class InfluxDBHelperTests {

    RestTemplate restTemplateMock;
    RouteProvider routeProviderMock;
    MeterRegistry meterRegistry;
    InfluxDBFactory influxDBUtilsMock;
    Timer influxDBWriteTimer;
    Timer getInfluxDBInfoTimer;
    GCLineProtocolBackupService backupService;

    @Before
    public void setUp() throws IOException {
        restTemplateMock = mock(RestTemplate.class);
        routeProviderMock = mock(RouteProvider.class);
        meterRegistry = mock(MeterRegistry.class);
        influxDBUtilsMock = mock(InfluxDBFactory.class);
        influxDBWriteTimer = mock(Timer.class);
        getInfluxDBInfoTimer = mock(Timer.class);
        backupService = mock(GCLineProtocolBackupService.class);
        when(meterRegistry.timer("ingestion.influxdb.write")).thenReturn(influxDBWriteTimer);
        when(meterRegistry.timer("ingestion.routing.info.get")).thenReturn(getInfluxDBInfoTimer);
    }

    @Test
    public void ingestToInfluxDb_withExistingDatabaseAndRetPolicy_shouldSucceed() throws Exception {
        InfluxDBHelper influxDBHelper = new InfluxDBHelper(
                restTemplateMock, routeProviderMock, meterRegistry,
                influxDBUtilsMock, backupService, 100, 100);
        String tenantId = "hybrid:1667601";
        String measurement = "cpu";
        String databaseName = "existing_db";
        String rpName = "existing_rp";

        doReturn(getTenantRoutes(tenantId, databaseName, rpName))
                .when(routeProviderMock).getRoute(tenantId, measurement, restTemplateMock);

        System.out.println(String.format("Testing with tenantIdAndMeasurement:[%s]; databaseName:[%s]; rpName:[%s]",
                tenantId, databaseName, rpName));

        successfulIngestionTest(influxDBHelper,
                influxDBUtilsMock, tenantId, measurement, databaseName, rpName,
                influxDBWriteTimer, getInfluxDBInfoTimer);
    }

    @Test
    public void ingestToInfluxDb_withNonExistingDatabase_shouldCreateDatabase() throws Exception {
        InfluxDBHelper influxDBHelper = new InfluxDBHelper(
                restTemplateMock, routeProviderMock, meterRegistry, influxDBUtilsMock, backupService,
                100, 100);
        String tenantId = "hybrid:1667601";
        String measurement = "cpu";
        String databaseName = "non_existing_database";
        String rpName = "existing_rp";

        doReturn(getTenantRoutes(tenantId, databaseName, rpName))
                .when(routeProviderMock).getRoute(tenantId, measurement, restTemplateMock);

        successfulIngestionTest(influxDBHelper, influxDBUtilsMock,
                tenantId, measurement, databaseName, rpName, influxDBWriteTimer, getInfluxDBInfoTimer);
    }

    @Test
    public void ingestToInfluxDb_withExistingDatabaseNonExistingRetentionPolicy_shouldCreateRetentionPolicy()
            throws Exception {
        InfluxDBHelper influxDBHelper = new InfluxDBHelper(
                restTemplateMock, routeProviderMock, meterRegistry,
                influxDBUtilsMock, backupService, 100, 100);
        String tenantId = "hybrid:1667601";
        String measurement = "cpu";
        String databaseName = "existing_db";
        String rpName = "existing_rp";
        String nonExistingRetentionPolicyName = "non_existing_rp";

        /**
         * Here rpName is different from what routeProvider will provide. Goal is that these two values should
         * be different, so that it's creating a scenario where retention policy does not exist for a given database.
         */
        doReturn(getTenantRoutes(tenantId, databaseName, nonExistingRetentionPolicyName))
                .when(routeProviderMock).getRoute(tenantId, measurement, restTemplateMock);

        successfulIngestionTest(influxDBHelper, influxDBUtilsMock, tenantId, measurement, databaseName,
                nonExistingRetentionPolicyName, influxDBWriteTimer, getInfluxDBInfoTimer);
    }

    private void successfulIngestionTest(
            InfluxDBHelper influxDBHelper, InfluxDBFactory influxDBUtilsMock,
            String tenantId, String measurement, String databaseName, String rpName,
            Timer influxDBWriteTimer, Timer getInfluxDBInfoTimer) throws Exception {

        String payloadToIngestInInfluxDB = "valid payload";
        String rollupLevel = "full";
        String dbQueryString = "SHOW DATABASES";
        String rpQueryString = String.format("SHOW RETENTION POLICIES ON \"%s\"", databaseName);
        String createDbString =
                String.format("CREATE DATABASE \"non_existing_database\" WITH DURATION 5d NAME \"%s\"", rpName);
        String createRetPolicyString =
                String.format("CREATE RETENTION POLICY \"non_existing_rp\" " +
                        "ON \"%s\" DURATION 5d REPLICATION 1 DEFAULT", databaseName);


        InfluxDB influxDBMock = mock(InfluxDB.class);

        when(influxDBUtilsMock.getInfluxDB("http://valid_url:8086",
                100, 100)).thenReturn(influxDBMock);

        when(influxDBMock.query(new Query(dbQueryString, "")))
                .thenReturn(getQueryResultForExistingDatabase());

        when(influxDBMock.query(new Query(rpQueryString, databaseName)))
                .thenReturn(getQueryResultForExistingRetentionPolicy());

        when(influxDBMock.query(new Query(createDbString, "")))
                .thenReturn(getQueryResultForExistingDatabase());

        when(influxDBMock.query(new Query(createRetPolicyString, databaseName)))
                .thenReturn(getQueryResultForExistingRetentionPolicy());

        doNothing().when(getInfluxDBInfoTimer).record(anyLong(), any());
        doNothing().when(influxDBWriteTimer).record(anyLong(), any());
        doNothing().when(influxDBMock).write(anyString(), anyString(), any(), any(), anyString());

        influxDBHelper.ingestToInfluxDb(payloadToIngestInInfluxDB, tenantId, measurement, rollupLevel);

        verify(influxDBMock, Mockito.times(1)).write(
                databaseName, rpName, InfluxDB.ConsistencyLevel.ONE, TimeUnit.SECONDS, payloadToIngestInInfluxDB);
    }

    private TenantRoutes getTenantRoutes(String tenantId, String databaseName, String rpName) {
        TenantRoutes tenantRoutes = new TenantRoutes();
        tenantRoutes.setTenantIdAndMeasurement(tenantId);
        Map<String, TenantRoutes.TenantRoute> routes = tenantRoutes.getRoutes();

        routes.put("full", new TenantRoutes.TenantRoute(
                "http://valid_url:8086",
                databaseName,
                rpName,
                "5d"
        ));

        return tenantRoutes;
    }

    private QueryResult getQueryResultForExistingDatabase() {
        QueryResult.Series series = new QueryResult.Series();
        List<Object> seriesValues = new ArrayList<>();
        seriesValues.add("existing_db");
        List<List<Object>> seriesValuesList = new ArrayList<>();
        seriesValuesList.add(seriesValues);
        series.setValues(seriesValuesList);

        QueryResult.Result result = new QueryResult.Result();
        result.setSeries(Arrays.asList(series));

        QueryResult queryResult = new QueryResult();
        queryResult.setResults(Arrays.asList(result));

        return queryResult;
    }

    private QueryResult getQueryResultForExistingRetentionPolicy() {
        QueryResult.Series series = new QueryResult.Series();
        List<Object> seriesValues = new ArrayList<>();
        seriesValues.add("existing_rp");
        List<List<Object>> seriesValuesList = new ArrayList<>();
        seriesValuesList.add(seriesValues);
        series.setValues(seriesValuesList);

        QueryResult.Result result = new QueryResult.Result();
        result.setSeries(Arrays.asList(series));

        QueryResult queryResult = new QueryResult();
        queryResult.setResults(Arrays.asList(result));

        return queryResult;
    }
}
