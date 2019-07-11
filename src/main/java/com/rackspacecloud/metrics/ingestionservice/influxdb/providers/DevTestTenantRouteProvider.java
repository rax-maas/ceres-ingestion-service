package com.rackspacecloud.metrics.ingestionservice.influxdb.providers;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;

public class DevTestTenantRouteProvider implements RouteProvider {
    @Value("${tenant-routing-service.url}")
    private String tenantRoutingServiceUrl;

    @Override
    public TenantRoutes getRoute(String tenantId, String measurement, RestTemplate restTemplate) throws Exception {
        String requestUrl = String.format("%s/%s/%s", tenantRoutingServiceUrl, tenantId, measurement);

        try {
            return restTemplate.getForObject(requestUrl, TenantRoutes.class);
        }
        catch (Exception e) {
            throw new Exception(String.format("Exception thrown for requestUrl [%s]", requestUrl), e);
        }
    }
}
