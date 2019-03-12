package com.rackspacecloud.metrics.ingestionservice.listeners.processors;

import com.rackspacecloud.metrics.ingestionservice.influxdb.Point;
import lombok.Data;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

import static com.rackspacecloud.metrics.ingestionservice.utils.InfluxDBUtils.replaceSpecialCharacters;

@Data
public class Dimension {
    private static final String ACCOUNT_ID = "accountId";
    private static final String MONITORING_ZONE = "monitoringZone";
    private static final String ENTITY_ID = "entityId";
    private static final String CHECK_TYPE = "checkType";
    private static final String TENANT_ID = "tenantId";

    Map<String, String> systemMetadata;
    String collectionTarget;
    String monitoringSystem;
    String collectionLabel;
    String deviceLabel;

    public Dimension() {
        systemMetadata = new HashMap<>();
    }

    public static Point.Builder populateTagsAndFields(Dimension dimension) {

        String measurement = dimension.getSystemMetadata().get(CHECK_TYPE);
        if(measurement == null) return null;

        String tenantId = dimension.getSystemMetadata().get(TENANT_ID);
        if(tenantId == null) return null;

        Point.Builder pointBuilder = Point.measurement(replaceSpecialCharacters(measurement));

        if(!StringUtils.isEmpty(dimension.getSystemMetadata().get(ACCOUNT_ID))) {
            pointBuilder.tag("systemaccountid", dimension.getSystemMetadata().get(ACCOUNT_ID).trim());
        }

        if(!StringUtils.isEmpty(dimension.getCollectionTarget())) {
            pointBuilder.tag("target", dimension.getCollectionTarget().trim());
        }

        if(!StringUtils.isEmpty(dimension.getMonitoringSystem())) {
            pointBuilder.tag("monitoringsystem", dimension.getMonitoringSystem().trim());
        }

        if(!StringUtils.isEmpty(dimension.getCollectionLabel())) {
            pointBuilder.tag("collectionlabel", dimension.getCollectionLabel().trim());
        }

        addEntityTags(dimension, pointBuilder);
        addMonitoringZone(dimension, pointBuilder);

        return pointBuilder;
    }

    static void addMonitoringZone(Dimension dimension, Point.Builder pointBuilder) {
        final String monitoringZone = dimension.getSystemMetadata().get(MONITORING_ZONE);
        if(!StringUtils.isEmpty(monitoringZone)){
            pointBuilder.tag("monitoringzone", monitoringZone.trim());
        }
    }

    static void addEntityTags(Dimension dimension, Point.Builder pointBuilder) {
        final String entityId = dimension.getSystemMetadata().get(ENTITY_ID);
        if(!StringUtils.isEmpty(entityId)) {
            pointBuilder.tag("entitysystemid", entityId.trim());
        }
        if(!StringUtils.isEmpty(dimension.getDeviceLabel())) {
            pointBuilder.tag("devicelabel", dimension.getDeviceLabel().trim());
        }
    }
}
