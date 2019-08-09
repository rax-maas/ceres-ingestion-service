package com.rackspacecloud.metrics.ingestionservice.utils;

import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;

public class InfluxDBFactory {
    public InfluxDB getInfluxDB(String url, int numberOfPointsInAWriteBatch, int writeFlushDurationMsLimit) {
        BatchOptions batchOptions = BatchOptions.DEFAULTS;
        batchOptions.actions(numberOfPointsInAWriteBatch);
        batchOptions.flushDuration(writeFlushDurationMsLimit);
        batchOptions.jitterDuration(200);

        InfluxDB influxDB = org.influxdb.InfluxDBFactory.connect(url);
        influxDB.setLogLevel(InfluxDB.LogLevel.BASIC);
        influxDB.enableBatch(batchOptions);
        return influxDB;
    }
}
