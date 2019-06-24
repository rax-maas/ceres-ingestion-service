package com.rackspacecloud.metrics.ingestionservice.influxdb;

import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.rackspacecloud.metrics.ingestionservice.influxdb.config.BackupProperties;
import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.LineProtocolBackupService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.channels.Channels;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

@Service
@EnableConfigurationProperties(BackupProperties.class)
@Slf4j
public class GCLineProtocolBackupService implements LineProtocolBackupService {

    // Reusable thread-safe date-time formatter for files going in a bucket
    private static final DateTimeFormatter dateBucketFormat = DateTimeFormatter.ofPattern("yyyyMMdd")
            .withZone(ZoneOffset.UTC);
    // The underlying blob storage. Dev/test will uses in-memory storage
    private final Storage storage;
    // The bucket where blobs will go
    private final String cloudOutputBucket;

    // Internal methods of this class attempt to talk to the Cache proxy
    // instead of the GCLineProtocolBackupService object
    private LineProtocolBackupService self;

    private BackupProperties backupProperties;

    @Resource
    public void setLineProtocolBackupService(LineProtocolBackupService self) {
        this.self = self;
    }

    // To autowire Storage in prod we'll need to get an account and point
    // GOOGLE_APPLICATION_CREDENTIALS to the json creds file
    // for a service account
    // as per https://www.baeldung.com/java-google-cloud-storage
    @Autowired
    public GCLineProtocolBackupService(Storage storage, BackupProperties backupProperties) {
        Assert.notNull(storage, "Storage must not be null");
        Assert.notNull(backupProperties, "The backup properties must not be null");
        // TODO: remove gcs prefix to make abstraction cleaner
        Assert.notNull(backupProperties.getGcsBackupBucket(), "The output bucket must not be null");
        this.storage = storage;
        this.cloudOutputBucket = backupProperties.getGcsBackupBucket();
        this.backupProperties = backupProperties;
    }

    /**
     * We want data to be stored in the following format: /[profile-specific
     * bucket]/[database name]/[retention policy
     * name]/[YYYYMMDD]/[UUID of file].gz
     *
     * File is line-protocol, gzipped, size may require some testing
     *
     * @param location the location within the bucket (directory) where blobs will be written to
     * @return the cached reference to the gzip output stream for that blob
     */
    @Cacheable(cacheNames = "lineProtocolBackupWriter", key="#location")
    public GZIPOutputStream getBackupStream(String location, String database, String retentionPolicy) throws IOException {
        Assert.hasText(location, "location must contain text");
        String fileName = location + "/" + UUID.randomUUID().toString() + ".gz";
        BlobId blobId = BlobId.of(cloudOutputBucket, fileName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/gzip").build();
        // enable flushing the compressor
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(Channels.newOutputStream(storage.writer(blobInfo)), true);
        log.info("Returning a new backup stream {} for {} in bucket {}", gzipOutputStream, blobInfo, cloudOutputBucket);

        // We need to add a header specifying where to import to make restore easier
        gzipOutputStream.write(("# DML\n" +
                "# CONTEXT-DATABASE: " + database + "\n" +
                "# CONTEXT-RETENTION-POLICY: " + retentionPolicy + "\n").getBytes());
        return gzipOutputStream;
    }

    @Override
    public void writeToBackup(String payload, URL instanceURL, String database, String retentionPolicy)
            throws IOException {
        Assert.hasText(payload, "payload must be a line protocol payload");
        Assert.hasText(database, "database name must not be missing");
        Assert.hasText(retentionPolicy, "retention policy name must not be missing");
        Assert.hasText(instanceURL.getHost(), "database hostname must be specified in the instance URL");
        GZIPOutputStream gzipOutputStream = self.getBackupStream(
                getBackupLocation(payload, instanceURL, database, retentionPolicy),
                database, retentionPolicy);
        Assert.notNull(gzipOutputStream, "Cache provided a null cloud stream");
        log.info("Writing to ({}, {},{}) using stream {}: {}", instanceURL.getHost(), database, retentionPolicy,
                gzipOutputStream, payload);
        synchronized(gzipOutputStream) {
            gzipOutputStream.write(payload.getBytes());
            gzipOutputStream.write("\n".getBytes()); // add line separator
            if(backupProperties.isAlwaysFlush()) gzipOutputStream.flush();
        }
    }

    @Override
    @PreDestroy
    @CacheEvict(value = "lineProtocolBackupWriter", allEntries = true)
    @Scheduled(fixedRateString = "${backup.gcs-flush-milliseconds}", initialDelay = 10000)
    public void flush() {
        log.debug("Clearing cache");
    }

    public static final RemovalListener removalListener = (RemovalListener<String, GZIPOutputStream>) (key, value, cause) -> {
        try {
            Assert.notNull(value, "Cache removed a null value");
            log.info("Closing stream {}", value);
            value.finish();
            value.close();
        } catch (IOException e) {
            // Exceptions in this method will be eliminated and logged automatically by spring
            throw new RuntimeException(e);
        }
    };

    private static final Pattern payloadTimestampPattern = Pattern.compile(" ([0-9]*)$");

    private static long parseTimestampFromPayload(String payload) {
        Matcher m = payloadTimestampPattern.matcher(payload);
        Assert.isTrue(m.find(), "Could not find timestamp in payload");
        return Long.valueOf(m.group(1));
    }

    /**
     * @param payload         The payload; used to extract timestamp
     * @param instanceURL     The URL of the influxdb instance
     * @param database        The database this payload would be going to
     * @param retentionPolicy The retention policy this payload would be written
     *                        under
     * @return the name of the new blob or file
     */
    public static String getBackupLocation(String payload, URL instanceURL, String database, String retentionPolicy) throws UnsupportedEncodingException {
        Assert.hasText(payload, "payload must be a line protocol payload");
        Assert.hasText(database, "database name must not be missing");
        Assert.hasText(retentionPolicy, "retention policy name must not be missing");
        return URLEncoder.encode(instanceURL.getHost(), "UTF-8") + "/" +
                URLEncoder.encode(database, "UTF-8") + "/" +
                URLEncoder.encode(retentionPolicy, "UTF-8") + "/"
                + dateBucketFormat.format(Instant.ofEpochSecond(parseTimestampFromPayload(payload)));
    }
}
