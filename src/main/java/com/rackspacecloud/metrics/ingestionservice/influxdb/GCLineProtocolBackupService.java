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
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.io.IOException;
import java.nio.channels.Channels;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

@Service
public class GCLineProtocolBackupService implements LineProtocolBackupService {

    // Reusable thread-safe date-time formatter for files going in a bucket
    private static final DateTimeFormatter dateBucketFormat = DateTimeFormatter.ofPattern("yyyyMMdd")
            .withZone(ZoneOffset.UTC);
    private static final Logger LOGGER = LoggerFactory.getLogger(GCLineProtocolBackupService.class);
    // The underlying blob storage. Dev/test will uses in-memory storage
    private final Storage storage;
    // The bucket where blobs will go
    private final String cloudOutputBucket;

    // Internal methods of this class attempt to talk to the Cache proxy
    // instead of the GCLineProtocolBackupService object
    @Resource
    private LineProtocolBackupService self;

    // To autowire Storage in prod we'll need to get an account and point
    // GOOGLE_APPLICATION_CREDENTIALS to the json creds file
    // for a service account
    // as per https://www.baeldung.com/java-google-cloud-storage
    @Autowired
    public GCLineProtocolBackupService(Storage storage, @Value("${backup.gcs-backup-bucket}") String cloudOutputBucket) {
        Assert.notNull(storage, "Storage must not be null");
        Assert.notNull(cloudOutputBucket, "The output bucket must not be null");
        this.storage = storage;
        this.cloudOutputBucket = cloudOutputBucket;
    }

    /**
     * We want data to be stored in the following format: /[profile-specific
     * bucket]/[instance name]/[database name]/[retention policy
     * name]/[YYYYMMDD]/[UUID of file].gz
     *
     * File is line-protocol, gzipped, size may require some testing
     *
     * @param location the bucket location (directory) of the file we are writing to
     * @return the cached reference to the gzip output stream for that blob
     */
    @Cacheable(cacheNames = "lineProtocolBackupWriter")
    public GZIPOutputStream getBackupStream(String location) throws IOException {
        Assert.notNull(location, "fileName must not be null");
        Assert.notNull(cloudOutputBucket, "bucket name was not injected properly");
        Assert.notNull(storage, "storage was not injected properly");
        String fileName = location + "/" + UUID.randomUUID().toString() + ".gz";
        BlobId blobId = BlobId.of(cloudOutputBucket, fileName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/gzip").build();
        // enable flushing the compressor
        return new GZIPOutputStream(Channels.newOutputStream(storage.writer(blobInfo)), true);
    }

    @Override
    public void writeToBackup(String payload, String instance, String database, String retentionPolicy)
            throws IOException {
        GZIPOutputStream gzipOutputStream = self.getBackupStream(getBackupLocation(payload, instance, database, retentionPolicy));
        synchronized(gzipOutputStream) {
            gzipOutputStream.write(payload.getBytes());
            gzipOutputStream.write("\n".getBytes()); // add separator
        }
    }

    @Override
    @CacheEvict(value = "lineProtocolBackupWriter", allEntries = true)
    public void clear() {
        LOGGER.debug("Clearing cache");
    }

    public static final RemovalListener removalListener = (RemovalListener<String, GZIPOutputStream>) (key, value, cause) -> {
        try {
            value.finish();
            value.close();
        } catch (IOException e) {
            // this will be eliminated and logged automatically
            throw new RuntimeException(e);
        }
    };

    private static final Pattern payloadTimestampPattern = Pattern.compile(" ([0-9]*)$");

    private static long parseTimestampFromPayload(String payload) {
        Matcher m = payloadTimestampPattern.matcher(payload);
        m.find();
        return Integer.valueOf(m.group(1));
    }

    /**
     * @param payload         The payload; used to extract timestamp
     * @param instance        The instance this payload would be going to
     * @param database        The database this payload would be going to
     * @param retentionPolicy The retention policy this payload would be written
     *                        under
     * @return the name of the new blob or file
     */
    public static String getBackupLocation(String payload, String instance, String database, String retentionPolicy) {
        return instance + "/" + database + "/" + retentionPolicy + "/"
                + dateBucketFormat.format(Instant.ofEpochSecond(parseTimestampFromPayload(payload)));
    }
}