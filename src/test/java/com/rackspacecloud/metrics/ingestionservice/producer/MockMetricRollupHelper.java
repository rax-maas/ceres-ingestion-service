package com.rackspacecloud.metrics.ingestionservice.producer;

import com.rackspacecloud.metrics.ingestionservice.listeners.rolluplisteners.models.MetricRollup;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class MockMetricRollupHelper {

    public static MetricRollup getValidMetricRollup(int i, String tenantId, boolean wantIValues){
        MetricRollup metricRollup = new MetricRollup();

        metricRollup.setKey("key_" + i);
        metricRollup.setStart(123456L);
        metricRollup.setEnd(987654L);
        metricRollup.setAccount("1234567");
        metricRollup.setAccountType("CORE");
        metricRollup.setDevice((1000 + i) + "");
        metricRollup.setDeviceLabel("dummy-device-label-" + i);
        metricRollup.setDeviceMetadata(new HashMap<>());
        metricRollup.setMonitoringSystem("MAAS");

        Map<String, String> systemMetadata = new HashMap<>();
        systemMetadata.put("checkType", "agent.filesystem");
        systemMetadata.put("tenantId", tenantId);
        systemMetadata.put("accountId", "dummy-account-id-" + i);
        systemMetadata.put("entityId", "dummy-entity-id-" + i);
        systemMetadata.put("checkId", "dummy-check-id-" + i);
        systemMetadata.put("monitoringZone", "");
        metricRollup.setSystemMetadata(systemMetadata);

        metricRollup.setCollectionLabel("dummy-collection-label");
        metricRollup.setCollectionTarget("");

        Map<String, String> collectionMetadata = new HashMap<>();
        collectionMetadata.put("rpc_maas_version", "1.7.7");
        collectionMetadata.put("rpc_maas_deploy_date", "2018-10-04");
        collectionMetadata.put("rpc_check_category", "host");
        collectionMetadata.put("product", "osa");
        collectionMetadata.put("osa_version", "14.2.4");
        collectionMetadata.put("rpc_env_identifier", "as-c");
        metricRollup.setCollectionMetadata(collectionMetadata);

        Map<String, MetricRollup.RollupBucket<Long>> iValues = new HashMap<>();

        if(wantIValues) iValues = getIValues();

        metricRollup.setIvalues(iValues);

        metricRollup.setFvalues(new HashMap<>());

        Map<String, String> units = new HashMap<>();
        units.put("filesystem.free_files", "free_files");
        units.put("filesystem.files", "files");
        units.put("filesystem.total", "KILOBYTES");
        units.put("filesystem.free", "KILOBYTES");
        units.put("filesystem.avail", "KILOBYTES");
        units.put("filesystem.used", "KILOBYTES");

        metricRollup.setUnits(units);

        return metricRollup;
    }

    private static Map<String, MetricRollup.RollupBucket<Long>> getIValues() {
        Map<String, MetricRollup.RollupBucket<Long>> iValues = new HashMap<>();

        iValues.put("filesystem.total", getRollupData());
        iValues.put("filesystem.free", getRollupData());
        iValues.put("filesystem.free_files", getRollupData());
        iValues.put("filesystem.avail", getRollupData());
        iValues.put("filesystem.files", getRollupData());
        iValues.put("filesystem.used", getRollupData());
        return iValues;
    }

    private static MetricRollup.RollupBucket getRollupData() {
        long tempVal = getNextLongValue();
        MetricRollup.RollupBucket bucket = new MetricRollup.RollupBucket<>();
        bucket.setMin(tempVal - 10);
        bucket.setMax(tempVal + 10);
        bucket.setMean(tempVal);
        return bucket;
    }

    private static long getNextLongValue() {
        return ThreadLocalRandom.current().nextLong(1000L, 50_000L);
    }

}
