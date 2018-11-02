package com.rackspacecloud.metrics.kafkainfluxdbconsumer.config;

import com.rackspacecloud.metrics.kafkainfluxdbconsumer.models.TenantRoute;
import com.rackspacecloud.metrics.kafkainfluxdbconsumer.providers.IRouteProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.web.client.RestTemplate;

@Configuration
@Profile("production")
public class ProdTenantRouteProvider implements IRouteProvider {
    @Value("${tenant-routing-service.url}")
    private String tenantRoutingServiceUrl;

    @Override
    public TenantRoute getRoute(String tenantId, RestTemplate restTemplate) {
        String requestUrl = String.format("%s/%s", tenantRoutingServiceUrl, tenantId);

        //TODO: Work on any exception handling if restTemplate throws exception
        return restTemplate.getForObject(requestUrl, TenantRoute.class);
    }
}
