package com.rackspacecloud.metrics.ingestionservice.influxdb.providers;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * This model keeps tenant routes for a tenantId and measurement duo.
 */
@Data
public class TenantRoutes {
    // This is the key for the routes. tenantId + measurement is one atomic unit (key).
    private String tenantIdAndMeasurement;

    // This map contains all of the routes. Key is the aggregation level and value is the route for that
    // aggregation level. Aggregation level maps to the retention-policy in the database.
    // For example: "full" contains the raw data which is routed to "rp_5d". "5m" aggregated data is routed to "rp_10d".
    private Map<String, TenantRoute> routes;

    public TenantRoutes() {
        routes = new HashMap<>();
    }

    @Data
    @RequiredArgsConstructor
    public static class TenantRoute {
        private String path; // InfluxDB instance url. For example: "http://localhost:8086"
        private String databaseName;
        private String retentionPolicyName;
        private String retentionPolicy;

        public TenantRoute(String path, String databaseName, String retentionPolicyName,
                           String retentionPolicy){
            this.path = path;
            this.databaseName = databaseName;
            this.retentionPolicyName = retentionPolicyName;
            this.retentionPolicy = retentionPolicy;
        }
    }
}
