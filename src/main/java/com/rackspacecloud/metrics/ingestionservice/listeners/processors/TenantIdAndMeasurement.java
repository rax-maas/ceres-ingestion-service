package com.rackspacecloud.metrics.ingestionservice.listeners.processors;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class TenantIdAndMeasurement {
    String tenantId;
    String measurement;

    public TenantIdAndMeasurement(String tenantId, String measurement) {
        this.tenantId = tenantId;
        this.measurement = measurement;
    }
}
