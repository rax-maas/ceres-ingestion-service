package com.rackspacecloud.metrics.ingestionservice.influxdb.providers;

import org.springframework.web.client.RestTemplate;

/**
 * Route provider provides routes for given tenantId and measurement.
 */
public interface RouteProvider {
    /**
     * This method provide all of the routes for given tenantId and measurement.
     * @param tenantId
     * @param measurement
     * @param restTemplate is used to connect to the routing service to get the route
     * @return TenantRoutes
     * @throws Exception
     */
    TenantRoutes getRoute(String tenantId, String measurement, RestTemplate restTemplate) throws Exception;
}
