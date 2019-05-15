package com.rackspacecloud.metrics.ingestionservice.influxdb.providers;

import java.io.IOException;

public interface LineProtocolBackupService {
    void writeToBackup(String payload, String instance, String database, String retentionPolicy) throws IOException;
}
