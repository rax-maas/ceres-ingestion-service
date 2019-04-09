package com.rackspacecloud.metrics.ingestionservice.influxdb.providers;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;

public class DevTestTenantRouteProvider implements RouteProvider {
    @Value("${tenant-routing-service.url}")
    private String tenantRoutingServiceUrl;

    @Override
    public TenantRoutes getRoute(String tenantId, String measurement, RestTemplate restTemplate) {
        String requestUrl = String.format("%s/%s/%s", tenantRoutingServiceUrl, tenantId, measurement);

        //TODO: Work on any exception handling if restTemplate throws exception
        return restTemplate.getForObject(requestUrl, TenantRoutes.class);
    }
}
