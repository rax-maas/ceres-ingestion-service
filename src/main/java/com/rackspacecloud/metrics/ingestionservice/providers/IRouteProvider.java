package com.rackspacecloud.metrics.ingestionservice.providers;

import com.rackspacecloud.metrics.ingestionservice.models.TenantRoute;
import org.springframework.web.client.RestTemplate;

public interface IRouteProvider {
    TenantRoute getRoute(String tenantId, RestTemplate restTemplate);
}
