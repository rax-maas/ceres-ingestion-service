package com.rackspacecloud.metrics.ingestionservice.models;

import lombok.Data;

@Data
public class TenantRoute {
    private String path;
    private String databaseName;
}
