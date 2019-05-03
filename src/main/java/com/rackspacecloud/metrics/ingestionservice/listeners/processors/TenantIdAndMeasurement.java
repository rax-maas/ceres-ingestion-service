package com.rackspacecloud.metrics.ingestionservice.listeners.processors;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TenantIdAndMeasurement {
    String tenantId;
    String measurement;
}
