package com.rackspacecloud.metrics.ingestionservice.influxdb.providers;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;

/**
 * TenantRouteProvider for Dev/Test related work. This class removes Routing-Service dependency.
 */
public class DevTestTenantRouteProvider implements RouteProvider {
    @Value("${tenant-routing-service.url}")
    private String tenantRoutingServiceUrl;

    /**
     * Get stubbed tenant routes.
     * @param tenantId
     * @param measurement
     * @param restTemplate is used to connect to the routing service to get the route
     * @return
     * @throws Exception
     */
    @Override
    public TenantRoutes getRoute(String tenantId, String measurement, RestTemplate restTemplate) throws Exception {
        String requestUrl = String.format("%s/%s/%s", tenantRoutingServiceUrl, tenantId, measurement);

        try {
            return getStubbedRoutes(tenantId, measurement);
        }
        catch (Exception e) {
            throw new Exception(String.format("Exception thrown for requestUrl [%s]", requestUrl), e);
        }
    }

    /**
     * This method is a stub to generate the routes. Used for dev and test related work to break the dev
     * dependency on routing-service.
     * @param tenantId
     * @param measurement
     * @return
     */
    private TenantRoutes getStubbedRoutes(String tenantId, String measurement) {
        String tenantIdAndMeasurement = String.format("%s:%s", tenantId, measurement);

        TenantRoutes tenantRoutes = new TenantRoutes();
        tenantRoutes.setTenantIdAndMeasurement(tenantIdAndMeasurement);

        tenantRoutes.getRoutes().put("full", new TenantRoutes.TenantRoute(
                "http://localhost:8086",
                "db_0",
                "rp_5d",
                "5d"
        ));

        tenantRoutes.getRoutes().put("5m", new TenantRoutes.TenantRoute(
                "http://localhost:8086",
                "db_0",
                "rp_10d",
                "10d"
        ));

        tenantRoutes.getRoutes().put("20m", new TenantRoutes.TenantRoute(
                "http://localhost:8086",
                "db_0",
                "rp_20d",
                "20d"
        ));

        tenantRoutes.getRoutes().put("60m", new TenantRoutes.TenantRoute(
                "http://localhost:8086",
                "db_0",
                "rp_155d",
                "155d"
        ));

        tenantRoutes.getRoutes().put("240m", new TenantRoutes.TenantRoute(
                "http://localhost:8086",
                "db_0",
                "rp_300d",
                "300d"
        ));

        tenantRoutes.getRoutes().put("1440m", new TenantRoutes.TenantRoute(
                "http://localhost:8086",
                "db_0",
                "rp_1825d",
                "1825d"
        ));

        return tenantRoutes;
    }
}
