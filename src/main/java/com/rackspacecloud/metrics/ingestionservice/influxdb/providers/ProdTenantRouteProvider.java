package com.rackspacecloud.metrics.ingestionservice.influxdb.providers;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;

/**
 * This class provides tenant routes for given tenantId and measurement.
 */
public class ProdTenantRouteProvider implements RouteProvider {
    @Value("${tenant-routing-service.url}")
    private String tenantRoutingServiceUrl;

    /**
     * This method calls routing-service to get the routes for given tenantId and measurement.
     * @param tenantId
     * @param measurement
     * @param restTemplate is used to connect to the routing service to get the route
     * @return
     */
    @Override
    public TenantRoutes getRoute(String tenantId, String measurement, RestTemplate restTemplate) {
        String requestUrl = String.format("%s/%s/%s", tenantRoutingServiceUrl, tenantId, measurement);

        return restTemplate.getForObject(requestUrl, TenantRoutes.class);

    }
}
