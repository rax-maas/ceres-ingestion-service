package com.rackspacecloud.metrics.ingestionservice.influxdb.providers;

import org.springframework.web.client.RestTemplate;

public interface RouteProvider {
    TenantRoutes getRoute(String tenantId, RestTemplate restTemplate);
}
