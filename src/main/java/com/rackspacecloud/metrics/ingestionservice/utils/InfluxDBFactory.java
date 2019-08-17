package com.rackspacecloud.metrics.ingestionservice.utils;

import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;

public class InfluxDBFactory {
    public InfluxDB getInfluxDB(String url, int numberOfPointsInAWriteBatch,
                                int writeFlushDurationMsLimit, int jitterDuration) {
        BatchOptions batchOptions = BatchOptions.DEFAULTS;
        if(numberOfPointsInAWriteBatch > 0) batchOptions.actions(numberOfPointsInAWriteBatch);
        if(writeFlushDurationMsLimit > 0) batchOptions.flushDuration(writeFlushDurationMsLimit);
        if(jitterDuration > 0) batchOptions.jitterDuration(jitterDuration);

        InfluxDB influxDB = org.influxdb.InfluxDBFactory.connect(url);
        influxDB.setLogLevel(InfluxDB.LogLevel.BASIC);
        influxDB.enableBatch(batchOptions);
        return influxDB;
    }

    public InfluxDB getInfluxDB(String url) {
        return getInfluxDB(url, 0, 0, 0);
    }
}
