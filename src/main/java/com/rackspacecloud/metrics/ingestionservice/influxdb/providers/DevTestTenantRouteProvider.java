package com.rackspacecloud.metrics.ingestionservice.influxdb.providers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;

public class DevTestTenantRouteProvider implements RouteProvider {
    private final static String ROUTES = "{\n" +
            "  \"tenantId\": \"hybrid_1667601\",\n" +
            "  \"routes\": {\n" +
            "    \"60m\": {\n" +
            "      \"path\": \"http://data-influxdb:8086\",\n" +
            "      \"databaseName\": \"db_hybrid_1667601\",\n" +
            "      \"retentionPolicyName\": \"rp_155d\",\n" +
            "      \"retentionPolicy\": \"155d\",\n" +
            "      \"maxSeriesCount\": 10000\n" +
            "    },\n" +
            "    \"240m\": {\n" +
            "      \"path\": \"http://data-influxdb:8086\",\n" +
            "      \"databaseName\": \"db_hybrid_1667601\",\n" +
            "      \"retentionPolicyName\": \"rp_300d\",\n" +
            "      \"retentionPolicy\": \"300d\",\n" +
            "      \"maxSeriesCount\": 10000\n" +
            "    },\n" +
            "    \"full\": {\n" +
            "      \"path\": \"http://data-influxdb:8086\",\n" +
            "      \"databaseName\": \"db_hybrid_1667601\",\n" +
            "      \"retentionPolicyName\": \"rp_5d\",\n" +
            "      \"retentionPolicy\": \"5d\",\n" +
            "      \"maxSeriesCount\": 10000\n" +
            "    },\n" +
            "    \"1440m\": {\n" +
            "      \"path\": \"http://data-influxdb:8086\",\n" +
            "      \"databaseName\": \"db_hybrid_1667601\",\n" +
            "      \"retentionPolicyName\": \"rp_1825d\",\n" +
            "      \"retentionPolicy\": \"1825d\",\n" +
            "      \"maxSeriesCount\": 10000\n" +
            "    },\n" +
            "    \"5m\": {\n" +
            "      \"path\": \"http://data-influxdb:8086\",\n" +
            "      \"databaseName\": \"db_hybrid_1667601\",\n" +
            "      \"retentionPolicyName\": \"rp_10d\",\n" +
            "      \"retentionPolicy\": \"10d\",\n" +
            "      \"maxSeriesCount\": 10000\n" +
            "    },\n" +
            "    \"20m\": {\n" +
            "      \"path\": \"http://data-influxdb:8086\",\n" +
            "      \"databaseName\": \"db_hybrid_1667601\",\n" +
            "      \"retentionPolicyName\": \"rp_20d\",\n" +
            "      \"retentionPolicy\": \"20d\",\n" +
            "      \"maxSeriesCount\": 10000\n" +
            "    }\n" +
            "  }\n" +
            "}";

    @Override
    public TenantRoutes getRoute(String tenantId, RestTemplate restTemplate){
        ObjectMapper mapper = new ObjectMapper();
        try {
            TenantRoutes routes = mapper.readValue(ROUTES, TenantRoutes.class);
            return routes;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
