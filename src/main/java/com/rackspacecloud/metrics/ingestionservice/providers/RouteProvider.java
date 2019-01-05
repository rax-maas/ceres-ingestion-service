package com.rackspacecloud.metrics.ingestionservice.providers;

import com.rackspacecloud.metrics.ingestionservice.models.TenantRoutes;
import org.springframework.web.client.RestTemplate;

public interface RouteProvider {
    TenantRoutes getRoute(String tenantId, RestTemplate restTemplate);
}
