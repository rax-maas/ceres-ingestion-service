package com.rackspacecloud.metrics.ingestionservice.influxdb;

import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.LineProtocolBackupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.channels.Channels;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

@Component
public class GCLineProtocolBackupService implements LineProtocolBackupService {

    // Reusable thread-safe date-time formatter for files going in a bucket
    private static DateTimeFormatter dateBucketFormat = DateTimeFormatter.ofPattern("yyyyMMdd");

    private static Pattern payloadTimestampPattern = Pattern.compile(" ([\\d]*)^");

    private static final Logger LOGGER = LoggerFactory.getLogger(GCLineProtocolBackupService.class);

    /**
     * We want data to be stored in the following format:
     * /[profile-specific bucket]/[instance name]/[database name]/[retention policy name]/[YYYYMMDD]/[UUID of file].gz
     *
     * File is line-protocol, gzipped, size may require some testing
     * @param fileName the full name of the file we are writing to
     * @return the cached reference to the gzip output stream for that blob
     */
    @Cacheable(value = "lineProtocolBackupWriter", key = "fileName")
    public GZIPOutputStream getBackupStream(String fileName) throws IOException {
        // Instantiates a client
        Storage storage = StorageOptions.getDefaultInstance().getService();

        // The name for the new bucket
        // TODO: param
        String bucketName = "ceres-backup-dev/";
        BlobId blobId = BlobId.of(bucketName, fileName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/gzip").build();
        return new GZIPOutputStream(Channels.newOutputStream(storage.writer(blobInfo)));
        // return new GZIPOutputStream(Channels.newOutputStream(storage.writer(blobInfo)), true); // to enable flushing the compressor
    }

    @Override
    public void writeToBackup(String payload, String instance, String database, String retentionPolicy) throws IOException {
        getBackupStream(getBackupFilename(payload, instance, database, retentionPolicy)).write(payload.getBytes());
    }

    public static RemovalListener removalListener = (RemovalListener<String, GZIPOutputStream>) (key, value, cause) -> {
        try {
            value.finish();
        } catch (IOException e) {
            // this will be eliminated and logged automatically
            throw new RuntimeException(e);
        }
    };

    private static long parseTimestampFromPayload(String payload) {
        Matcher m = payloadTimestampPattern.matcher(payload);
        m.find();
        return Integer.valueOf(m.group(1));
    }

    private static LocalUUID uuidGenerator = new LocalUUID();
    /**
     * @param payload           The payload; used to extract timestamp
     * @param instance          The instance this payload would be going to
     * @param database          The database this payload would be going to
     * @param retentionPolicy   The retention policy this payload would be written under
     * @return the name of the new blob or file
     */
    public static String getBackupFilename(String payload, String instance, String database, String retentionPolicy) {
        return instance + "/" +
                database + "/" +
                retentionPolicy + "/" +
                dateBucketFormat.format(Instant.ofEpochSecond(parseTimestampFromPayload(payload))) + "/" +
                uuidGenerator.generateUUID().toString() + ".gz";
    }
}
