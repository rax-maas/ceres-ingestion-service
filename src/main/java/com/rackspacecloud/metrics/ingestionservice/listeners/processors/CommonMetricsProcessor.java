package com.rackspacecloud.metrics.ingestionservice.listeners.processors;

import com.rackspace.monplat.protocol.ExternalMetric;
import com.rackspacecloud.metrics.ingestionservice.listeners.UnifiedMetricsListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

@Slf4j
public class CommonMetricsProcessor {
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

    /**
     * Create TenantId and measurement values from passed parameters
     * @param accountType
     * @param account
     * @param monitoringSystem
     * @param collectionName
     * @return
     */
    public static TenantIdAndMeasurement getTenantIdAndMeasurement(
            String accountType, String account, String monitoringSystem, String collectionName) {

        if(!isValid(ACCOUNT_TYPE, accountType)) {
            log.error("Invalid account type [{}] in the received record", accountType);
            throw new IllegalArgumentException(String.format("Invalid account type: [%s]", accountType));
        }

        if(!isValid(ACCOUNT, account)) {
            String oldName = account;
            account = UnifiedMetricsListener.replaceSpecialCharacters(account);

            log.warn("Changed account from [{}] to [{}]", oldName, account);
        }

        String tenantId = String.format("%s-%s", accountType, account);

        if(!isValid(MONITORING_SYSTEM, monitoringSystem)) {
            log.error("Invalid monitoring system [{}] in the received record", monitoringSystem);
            throw new IllegalArgumentException(String.format("Invalid monitoring system: [%s]", monitoringSystem));
        }

        if(!isValid(COLLECTION_NAME, collectionName)) {
            String oldCollectionName = collectionName;
            collectionName = UnifiedMetricsListener.replaceSpecialCharacters(collectionName);

            log.warn("Changed collectionName from [{}] to [{}]", oldCollectionName, collectionName);
        }

        String measurement = String.format("%s_%s", monitoringSystem, collectionName);

        return new TenantIdAndMeasurement(tenantId, measurement);
    }

    /**
     *  Field value should not contain any whitespace
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static boolean isValid(String fieldName, CharSequence fieldValue) {
        if (!StringUtils.containsWhitespace(fieldValue)) return true;

        log.error(
                "FieldValue [{}] is either empty or contains a whitespace for the field [{}]",
                fieldValue, fieldName
        );

        return false;
    }
}
