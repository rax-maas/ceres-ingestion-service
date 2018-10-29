package com.rackspacecloud.metrics.kafkainfluxdbconsumer.models;

import lombok.Data;

@Data
public class TenantRoute {
    private String path;
    private String databaseName;
}
