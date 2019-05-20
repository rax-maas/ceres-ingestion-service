package com.rackspacecloud.metrics.ingestionservice.influxdb.providers;

import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public interface LineProtocolBackupService {
    GZIPOutputStream getBackupStream(String location) throws IOException;
    void writeToBackup(String payload, String instance, String database, String retentionPolicy) throws IOException;
    void clear();
}
