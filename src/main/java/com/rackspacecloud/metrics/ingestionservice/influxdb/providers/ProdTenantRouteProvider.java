package com.rackspacecloud.metrics.ingestionservice.influxdb.providers;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;

public class ProdTenantRouteProvider implements RouteProvider {
    @Value("${tenant-routing-service.url}")
    private String tenantRoutingServiceUrl;

    @Override
    public TenantRoutes getRoute(String tenantId, RestTemplate restTemplate) {
        String requestUrl = String.format("%s/%s", tenantRoutingServiceUrl, tenantId);

        //TODO: Work on any exception handling if restTemplate throws exception
        return restTemplate.getForObject(requestUrl, TenantRoutes.class);
    }
}
