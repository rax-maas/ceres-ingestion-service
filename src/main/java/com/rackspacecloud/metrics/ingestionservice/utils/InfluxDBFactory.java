package com.rackspacecloud.metrics.ingestionservice.utils;

import org.influxdb.InfluxDB;

public class InfluxDBFactory {
    public InfluxDB getInfluxDB(String url) {
        return org.influxdb.InfluxDBFactory.connect(url);
    }
}
