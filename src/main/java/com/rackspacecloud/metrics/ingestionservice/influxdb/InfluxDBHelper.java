package com.rackspacecloud.metrics.ingestionservice.influxdb;

import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.LineProtocolBackupService;
import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.RouteProvider;
import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.TenantRoutes;
import com.rackspacecloud.metrics.ingestionservice.utils.InfluxDBFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.springframework.web.client.RestTemplate;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class InfluxDBHelper {
    /**
     * This map contains all of the InfluxDB related information for given tenantId and measurement
     * key = tenantId:measurement. Example: "CORE-123456:MAAS_agent_filesystem"
     * value = Map of rollupLevel and their path information
     *      Example:
     *          key = rollupLevel. Example: 60m
     *          value = Path info. Example:
     *              path = "http://data-influxdb-1:8086"
     *              databaseName = "db_6"
     *              retentionPolicyName = "rp_5d"
     *              retentionPolicy = "5d"
     */
    private ConcurrentMap<String, Map<String, InfluxDbInfoForRollupLevel>> influxDbInfoMap;
    private RestTemplate restTemplate;
    private RouteProvider routeProvider;
    private InfluxDBFactory influxDBFactory;
    private int numberOfPointsInAWriteBatch;
    private int writeFlushDurationMsLimit;
    private int jitterDuration;
    private ConcurrentMap<String, InfluxDB> urlInfluxDBInstanceMap;
    Timer influxDBWriteTimer;
    private LineProtocolBackupService backupService;

    // This timer captures the latency for getting data from routing service if it's trying
    // to get the data first time. Once it has the routing information from routing service,
    // it caches it.
    Timer getInfluxDBInfoTimer;

    public InfluxDBHelper(
            RestTemplate restTemplate, RouteProvider routeProvider, MeterRegistry registry,
            InfluxDBFactory influxDBFactory,
            LineProtocolBackupService backupService,
            int numberOfPointsInAWriteBatch, int writeFlushDurationMsLimit, int jitterDuration){
        this.restTemplate = restTemplate;
        this.routeProvider = routeProvider;
        this.influxDBFactory = influxDBFactory;
        this.influxDbInfoMap = new ConcurrentHashMap<>();
        this.urlInfluxDBInstanceMap = new ConcurrentHashMap<>();
        this.numberOfPointsInAWriteBatch = numberOfPointsInAWriteBatch;
        this.writeFlushDurationMsLimit = writeFlushDurationMsLimit;
        this.jitterDuration = jitterDuration;
        this.backupService = backupService;

        this.influxDBWriteTimer = registry.timer("ingestion.influxdb.write");
        this.getInfluxDBInfoTimer = registry.timer("ingestion.routing.info.get");
    }

    public InfluxDBFactory getInfluxDBFactory() {
        return this.influxDBFactory;
    }

    @Data
    @AllArgsConstructor
    class InfluxDbInfoForRollupLevel {
        private String path;
        private String databaseName;
        private String retentionPolicyName;
        private String retentionPolicy;
    }

    /**
     * Get Map of rollupLevel and their path information
     *      Example:
     *          key = rollupLevel. Example: 60m
     *          value = Path info. Example:
     *              path = "http://data-influxdb-1:8086"
     *              databaseName = "db_6"
     *              retentionPolicyName = "rp_5d"
     *              retentionPolicy = "5d"
     * @param tenantId
     * @param measurement
     * @return
     * @throws Exception
     */
    private Map<String, InfluxDbInfoForRollupLevel> getInfluxDbInfo(
            final String tenantId, final String measurement) throws Exception {

        String tenantIdAndMeasurementKey = String.format("%s:%s", tenantId, measurement);

        // If we already have routing information from earlier calls, we don't need to call
        // routing service to get the same information again
        if(influxDbInfoMap.containsKey(tenantIdAndMeasurementKey))
            return influxDbInfoMap.get(tenantIdAndMeasurementKey);

        // Get tenant routes (each rollup level and their corresponding path, dbname, ret-policy info)
        // from routing service
        TenantRoutes tenantRoutes = getTenantRoutes(tenantId, measurement);

        Map<String, InfluxDbInfoForRollupLevel> influxDbInfoForTenant = new HashMap<>();

        // Since request for this tenantId and measurement came first time, we need to make sure
        // all of databases and retention policies are created if they don't exist already
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
                    log.debug("Database {} and retention policy {} already exist", databaseName, retPolicyName);
                    influxDbInfoForTenant.put(rollupLevel, new InfluxDbInfoForRollupLevel(
                            path, databaseName, retPolicyName, retPolicy
                    ));
                }
                else { // Create retention policy
                    boolean isDefault = false;
                    if(rollupLevel.equalsIgnoreCase("full")) isDefault = true;

                    if(createRetentionPolicy(databaseName, path, retPolicy, retPolicyName, isDefault)) {
                        log.info("Created new retention policy named [{}] for database [{}] in instance [{}]",
                                retPolicyName, databaseName, path);

                        influxDbInfoForTenant.put(rollupLevel, new InfluxDbInfoForRollupLevel(
                                path, databaseName, retPolicyName, retPolicy
                        ));
                    }
                    else {
                        log.error("Failed to create retention policy {} on database {} in instance [{}]",
                                retPolicyName, databaseName, path);
                    }
                }
            }
            else {
                // TODO: store already processed database so that we don't call createDatabase blindly
                if(createDatabase(databaseName, path, retPolicy, retPolicyName)) {
                    log.info("Created new database [{}] with retention policy name [{}] on instance [{}]",
                            databaseName, retPolicyName, path);

                    influxDbInfoForTenant.put(rollupLevel, new InfluxDbInfoForRollupLevel(
                            path, databaseName, retPolicyName, retPolicy
                    ));
                }
                else {
                    log.error("Failed to create database [{}] with retention policy name [{}] on instance [{}]",
                            databaseName, retPolicyName, path);
                }
            }
        }

        influxDbInfoMap.put(tenantIdAndMeasurementKey, influxDbInfoForTenant);

        return influxDbInfoForTenant;
    }

    /**
     * Get tenant routes for given tenantId and measurement from routing service
     * @param tenantId
     * @param measurement
     * @return
     * @throws Exception
     */
    private TenantRoutes getTenantRoutes(String tenantId, String measurement) throws Exception {
        TenantRoutes tenantRoutes;

        try {
            tenantRoutes = routeProvider.getRoute(tenantId, measurement, restTemplate);
        }
        catch(Exception e) {
            String errMsg = String.format(
                    "Failed to get routes for tenantId [%s] and measurement [%s]", tenantId, measurement);
            log.error(errMsg, e);
            throw new Exception(errMsg, e);
        }

        if(tenantRoutes == null) throw new Exception("tenantRoutes is null.");
        return tenantRoutes;
    }

    private boolean databaseExists(final String databaseName, final String baseUrl) {
        String queryString = "SHOW DATABASES";

        InfluxDB influxDB = getInfluxDBClient(baseUrl);
        QueryResult queryResult = influxDB.query(new Query(queryString, ""));

        if(queryResult.hasError()) {
            log.error("Query result got error for query [{}]", queryString);
            return false;
        }

        if(queryResult != null
                && queryResult.getResults().size() > 0
                && queryResult.getResults().get(0).getSeries().size() > 0
                && queryResult.getResults().get(0).getSeries().get(0).getValues().size() > 0
                )
        {
            List<String> databases = new ArrayList<>();
            for(List<Object> strings : queryResult.getResults().get(0).getSeries().get(0).getValues()) {
                for(Object database : strings){
                    databases.add(database.toString());
                }
            }
            if(databases.contains(databaseName)) return true;
        }

        return false;
    }

    private boolean retentionPolicyExists(final String rp, final String databaseName, final String baseUrl) {
        String queryString = String.format("SHOW RETENTION POLICIES ON \"%s\"", databaseName);

        InfluxDB influxDB = getInfluxDBClient(baseUrl);

        QueryResult queryResult = influxDB.query(new Query(queryString, databaseName));

        if(queryResult.hasError()) {
            log.error("Query result got error for query [{}]", queryString);
            return false;
        }

        if(queryResult != null
                && queryResult.getResults().size() > 0
                && queryResult.getResults().get(0).getSeries().size() > 0
                && queryResult.getResults().get(0).getSeries().get(0).getValues().size() > 0
                )
        {
            List<String> retPolicies = new ArrayList<>();
            for(List<Object> strings : queryResult.getResults().get(0).getSeries().get(0).getValues()) {
                retPolicies.add(strings.get(0).toString());
            }

            if(retPolicies.contains(rp)) return true;
        }

        return false;
    }

    private boolean createDatabase(final String databaseName, final String baseUrl,
                                   final String retPolicy, final String retPolicyName) {
        String queryString = String.format("CREATE DATABASE \"%s\" WITH DURATION %s NAME \"%s\"",
                databaseName, retPolicy, retPolicyName);

        InfluxDB influxDB = getInfluxDBClient(baseUrl);

        QueryResult result = influxDB.query(new Query(queryString, ""));
        return !result.hasError();
    }

    /**
     * Get InfluxDB client for given InfluxDB instance
     * @param instanceUrl
     * @return
     */
    private InfluxDB getInfluxDBClient(String instanceUrl) {
        return this.urlInfluxDBInstanceMap.computeIfAbsent(instanceUrl, key -> this.influxDBFactory.getInfluxDB(
                instanceUrl, this.numberOfPointsInAWriteBatch,
                this.writeFlushDurationMsLimit, this.jitterDuration));
    }

    private boolean createRetentionPolicy(final String databaseName, final String baseUrl,
            final String retPolicy, final String retPolicyName, boolean isDefault) {
        String queryString = String.format("CREATE RETENTION POLICY \"%s\" ON \"%s\" DURATION %s REPLICATION 1",
                retPolicyName, databaseName, retPolicy);

        if(isDefault) queryString = queryString + " DEFAULT";

        InfluxDB influxDB = getInfluxDBClient(baseUrl);
        QueryResult result = influxDB.query(new Query(queryString, databaseName));

        return !result.hasError();
    }

    public void ingestToInfluxDb(
            String payload, String tenantId, String measurement, String rollupLevel) throws Exception {

        long startTimeGetInfluxDBInfo = System.currentTimeMillis();

        // Get db and URL info to route data to
        Map<String, InfluxDbInfoForRollupLevel> influxDbInfoForTenant = getInfluxDbInfo(tenantId, measurement);

        getInfluxDBInfoTimer.record(System.currentTimeMillis() - startTimeGetInfluxDBInfo, TimeUnit.MILLISECONDS);

        InfluxDbInfoForRollupLevel influxDbInfoForRollupLevel = influxDbInfoForTenant.get(rollupLevel);

        if(influxDbInfoForRollupLevel == null) return;

        String baseUrl = influxDbInfoForRollupLevel.getPath();
        String databaseName = influxDbInfoForRollupLevel.getDatabaseName();
        String retPolicyName = influxDbInfoForRollupLevel.getRetentionPolicyName();

        InfluxDB influxDB = getInfluxDBClient(baseUrl);

        long startTime = System.currentTimeMillis();
        try {
            influxDB.write(databaseName, retPolicyName, InfluxDB.ConsistencyLevel.ONE, TimeUnit.SECONDS, payload);
            backupService.writeToBackup(payload, new URL(baseUrl), databaseName, retPolicyName);
        }
        catch(InfluxDBException.PointsBeyondRetentionPolicyException ex) {
            log.error("Write failed for the payload. baseURL: [{}], databaseName: [{}], ret-policy: [{}]",
                    baseUrl, databaseName, retPolicyName);
            throw ex;
        }

        influxDBWriteTimer.record(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
    }
}
