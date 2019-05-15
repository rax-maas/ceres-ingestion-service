package com.rackspacecloud.metrics.ingestionservice.influxdb;

import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.LineProtocolBackupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.channels.Channels;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

@Service
public class GCLineProtocolBackupService implements LineProtocolBackupService {

    // Reusable thread-safe date-time formatter for files going in a bucket
    private static DateTimeFormatter dateBucketFormat = DateTimeFormatter.ofPattern("yyyyMMdd")
            .withZone(ZoneOffset.UTC);

    private static final Logger LOGGER = LoggerFactory.getLogger(GCLineProtocolBackupService.class);

    // TODO:
    // To autowire we'll need to get an account and point this to
    // GOOGLE_APPLICATION_CREDENTIALS
    // as per https://www.baeldung.com/java-google-cloud-storage
    @Autowired
    private Storage storage;

    @Value("${backup.gcs-backup-bucket}")
    private String cloudOutputBucket;

    /**
     * We want data to be stored in the following format: /[profile-specific
     * bucket]/[instance name]/[database name]/[retention policy
     * name]/[YYYYMMDD]/[UUID of file].gz
     *
     * File is line-protocol, gzipped, size may require some testing
     *
     * @param fileName the full name of the file we are writing to
     * @return the cached reference to the gzip output stream for that blob
     */
    @Cacheable(value = "lineProtocolBackupWriter", key = "fileName")
    public GZIPOutputStream getBackupStream(String fileName) throws IOException {
        BlobId blobId = BlobId.of(cloudOutputBucket, fileName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/gzip").build();
        return new GZIPOutputStream(Channels.newOutputStream(storage.writer(blobInfo)));
        // return new
        // GZIPOutputStream(Channels.newOutputStream(storage.writer(blobInfo)), true);
        // // to enable flushing the compressor
    }

    @Override
    public void writeToBackup(String payload, String instance, String database, String retentionPolicy)
            throws IOException {
        getBackupStream(getBackupFilename(payload, instance, database, retentionPolicy)).write(payload.getBytes());
    }

    public static RemovalListener removalListener = (RemovalListener<String, GZIPOutputStream>) (key, value, cause) -> {
        try {
            value.finish();
            value.close();
        } catch (IOException e) {
            // this will be eliminated and logged automatically
            throw new RuntimeException(e);
        }
    };

    private static Pattern payloadTimestampPattern = Pattern.compile(" ([0-9]*)$");

    private static long parseTimestampFromPayload(String payload) {
        Matcher m = payloadTimestampPattern.matcher(payload);
        m.find();
        return Integer.valueOf(m.group(1));
    }

    private static LocalUUID uuidGenerator = new LocalUUID();

    /**
     * @param payload         The payload; used to extract timestamp
     * @param instance        The instance this payload would be going to
     * @param database        The database this payload would be going to
     * @param retentionPolicy The retention policy this payload would be written
     *                        under
     * @return the name of the new blob or file
     */
    public static String getBackupFilename(String payload, String instance, String database, String retentionPolicy) {
        return instance + "/" + database + "/" + retentionPolicy + "/"
                + dateBucketFormat.format(Instant.ofEpochSecond(parseTimestampFromPayload(payload))) + "/"
                + uuidGenerator.generateUUID().toString() + ".gz";
    }
}
