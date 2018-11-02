package com.rackspacecloud.metrics.kafkainfluxdbconsumer.providers;

import com.rackspacecloud.metrics.kafkainfluxdbconsumer.models.TenantRoute;
import org.springframework.web.client.RestTemplate;

public interface IRouteProvider {
    TenantRoute getRoute(String tenantId, RestTemplate restTemplate);
}
