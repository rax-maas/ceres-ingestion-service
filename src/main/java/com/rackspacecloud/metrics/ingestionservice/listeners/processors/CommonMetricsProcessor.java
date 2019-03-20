package com.rackspacecloud.metrics.ingestionservice.listeners.processors;

import com.rackspace.monplat.protocol.ExternalMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

public class CommonMetricsProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommonMetricsProcessor.class);

    private static final String ACCOUNT_TYPE = "accountType";
    private static final String ACCOUNT = "account";
    private static final String MONITORING_SYSTEM = "monitoringSystem";
    private static final String COLLECTION_NAME = "collectionName";

    public static Dimension getDimensions(ExternalMetric record) {
        Dimension dimension = new Dimension();
        dimension.setAccountType(record.getAccountType().name());
        dimension.setAccount(record.getAccount());
        dimension.setDevice(record.getDevice());
        dimension.setDeviceLabel(record.getDeviceLabel());
        dimension.setDeviceMetadata(record.getDeviceMetadata());
        dimension.setMonitoringSystem(record.getMonitoringSystem().name());
        dimension.setSystemMetadata(record.getSystemMetadata());
        dimension.setCollectionName(record.getCollectionName());
        dimension.setCollectionLabel(record.getCollectionLabel());
        dimension.setCollectionTarget(record.getCollectionTarget());
        dimension.setCollectionMetadata(record.getCollectionMetadata());

        return dimension;
    }

    public static TenantIdAndMeasurement getTenantIdAndMeasurement(
            String accountType, String account, String monitoringSystem, String collectionName) {

        if(!isValid(ACCOUNT_TYPE, accountType)) {
            LOGGER.error("Invalid account type [{}] in the received record", accountType);
            throw new IllegalArgumentException(String.format("Invalid account type: [%s]", accountType));
        }

        if(!isValid(ACCOUNT, account)) {
            LOGGER.error("Invalid account [{}] in the received record", account);
            throw new IllegalArgumentException(String.format("Invalid account: [%s]", account));
        }

        String tenantId = String.format("%s-%s", accountType, account);

        if(!isValid(MONITORING_SYSTEM, monitoringSystem)) {
            LOGGER.error("Invalid monitoring system [{}] in the received record", monitoringSystem);
            throw new IllegalArgumentException(String.format("Invalid monitoring system: [%s]", monitoringSystem));
        }

        if(!isValid(COLLECTION_NAME, collectionName)) {
            LOGGER.error("Invalid collection name [{}] in the received record", collectionName);
            throw new IllegalArgumentException(String.format("Invalid collection name: [%s]", collectionName));
        }

        String measurement = String.format("%s_%s", monitoringSystem, collectionName);

        return new TenantIdAndMeasurement(tenantId, measurement);
    }

    public static boolean isValid(String fieldName, CharSequence fieldValue) {
        if (!StringUtils.containsWhitespace(fieldValue)) return true;

        LOGGER.error(
                "FieldValue [{}] is either empty or contains a whitespace for the field [{}]",
                fieldValue, fieldName
        );

        return false;
    }
}
