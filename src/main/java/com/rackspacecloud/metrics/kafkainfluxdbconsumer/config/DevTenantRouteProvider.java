package com.rackspacecloud.metrics.kafkainfluxdbconsumer.config;

import com.rackspacecloud.metrics.kafkainfluxdbconsumer.models.TenantRoute;
import com.rackspacecloud.metrics.kafkainfluxdbconsumer.providers.IRouteProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.web.client.RestTemplate;

@Configuration
@Profile("development")
public class DevTenantRouteProvider implements IRouteProvider {
    @Override
    public TenantRoute getRoute(String tenantId, RestTemplate restTemplate) {
        TenantRoute temp = new TenantRoute();
        temp.setPath("http://localhost:8086");
        temp.setDatabaseName("db_hybrid_1667601");
        return temp;
    }
}
