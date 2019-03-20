package com.rackspacecloud.metrics.ingestionservice.listeners.processors;

import com.rackspacecloud.metrics.ingestionservice.influxdb.Point;
import lombok.Data;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

@Data
public class Dimension {
    private static final String TENANT_ID = "tenantId";
    private static final String ACCOUNT_TYPE = "accountType";
    private static final String ACCOUNT = "account";
    private static final String DEVICE = "device";
    private static final String DEVICE_LABEL = "deviceLabel";
    private static final String MONITORING_SYSTEM = "monitoringSystem";
    private static final String COLLECTION_NAME = "collectionName";
    private static final String COLLECTION_LABEL = "collectionLabel";
    private static final String COLLECTION_TARGET = "collectionTarget";

    String accountType;
    String account;
    String device;
    String deviceLabel;
    Map<String, String> deviceMetadata;
    String monitoringSystem;
    Map<String, String> systemMetadata;
    String collectionName;
    String collectionLabel;
    String collectionTarget;
    Map<String, String> collectionMetadata;

    public Dimension() {
        deviceMetadata = new HashMap<>();
        systemMetadata = new HashMap<>();
        collectionMetadata = new HashMap<>();
    }

    public static Point.Builder populateTagsAndFields(
            Dimension dimension, TenantIdAndMeasurement tenantIdAndMeasurement) {

        if(dimension == null || tenantIdAndMeasurement == null) return null;

        String measurement = tenantIdAndMeasurement.getMeasurement();
        if(measurement == null) return null;

        String tenantId = tenantIdAndMeasurement.getTenantId();
        if(tenantId == null) return null;

        Point.Builder pointBuilder = Point.measurement(measurement);
        pointBuilder.tag(TENANT_ID, tenantId);

        // Account type is a mandatory field as this is part of tenantId
        pointBuilder.tag(ACCOUNT_TYPE, dimension.getAccountType().trim());

        // Account is a mandatory field as this is part of tenantId
        pointBuilder.tag(ACCOUNT, dimension.getAccount().trim());

        if(!StringUtils.isEmpty(dimension.getDevice())) {
            pointBuilder.tag(DEVICE, dimension.getDevice().trim());
        }

        if(!StringUtils.isEmpty(dimension.getDevice())) {
            pointBuilder.tag(DEVICE_LABEL, dimension.getDeviceLabel().trim());
        }

        Map<String, String> deviceMetadata = dimension.getDeviceMetadata();
        if(deviceMetadata != null) deviceMetadata.forEach((k, v) -> pointBuilder.tag(k, v));

        // Monitoring system is a mandatory field as this is part of measurement name
        pointBuilder.tag(MONITORING_SYSTEM, dimension.getMonitoringSystem().trim());

        Map<String, String> systemMetadata = dimension.getSystemMetadata();
        if(systemMetadata != null) systemMetadata.forEach((k, v) -> pointBuilder.tag(k, v));

        // Collection name is a mandatory field as this is part of measurement name
        pointBuilder.tag(COLLECTION_NAME, dimension.getCollectionName().trim());

        if(!StringUtils.isEmpty(dimension.getCollectionLabel())) {
            pointBuilder.tag(COLLECTION_LABEL, dimension.getCollectionLabel().trim());
        }

        if(!StringUtils.isEmpty(dimension.getCollectionTarget())) {
            pointBuilder.tag(COLLECTION_TARGET, dimension.getCollectionTarget().trim());
        }

        return pointBuilder;
    }
}
