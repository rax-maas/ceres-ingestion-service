package com.rackspacecloud.metrics.ingestionservice.influxdb.providers;

import java.io.IOException;
import java.net.URL;
import java.util.zip.GZIPOutputStream;

/**
 * Provides a service to back-up influxdb payloads using line-protocol.
 */
public interface LineProtocolBackupService {

    /**
     * @param location Specify a particular location (i.e. a "directory" within a bucket)
     * @return A backup stream where line protocol lines can be written to
     * @throws IOException
     */
    GZIPOutputStream getBackupStream(String location, String database, String retentionPolicy) throws IOException;

    /**
     * A helper method for accessing the backup service
     * @param payload The payload (line protocol payload that can be used for extracting a timestamp)
     * @param instanceURL The URL of the influxdb instance
     * @param database Database name used for the payload on the database instance
     * @param retentionPolicy Retention policy name on the database instance
     * @throws IOException
     */
    void writeToBackup(String payload, URL instanceURL, String database, String retentionPolicy) throws IOException;

    /**
     * Flushes and closes all buffers to the implemented backend and clears any caching.
     * New requests will generate new streams.
     */
    void flush();
}
