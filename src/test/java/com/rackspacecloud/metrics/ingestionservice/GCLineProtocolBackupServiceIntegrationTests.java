package com.rackspacecloud.metrics.ingestionservice;

import com.google.cloud.storage.StorageOptions;
import com.rackspacecloud.metrics.ingestionservice.influxdb.GCLineProtocolBackupService;
import com.rackspacecloud.metrics.ingestionservice.influxdb.config.BackupProperties;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.net.URL;
import java.util.zip.GZIPOutputStream;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles(value = { "test" })
@EmbeddedKafka(partitions = 1, topics = { IngestionServiceApplicationTests.UNIFIED_METRICS_TOPIC })
@EnableConfigurationProperties(BackupProperties.class)
public class GCLineProtocolBackupServiceIntegrationTests {

    @Autowired
    BackupProperties backupProperties;

    GCLineProtocolBackupService backupService;

    @Before
    public void setUp() {
        backupService = new GCLineProtocolBackupService(
                StorageOptions.getDefaultInstance().getService(), backupProperties);

        backupService.setLineProtocolBackupService(backupService);

        String value = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
        System.out.println(" Google application  data  "+value);
        Assume.assumeTrue("GOOGLE_APPLICATION_CREDENTIALS environment variable must be configured " +
                        "to point to a credentials file " +
                        "for a Google service account, but is " + value,
                value != null);

        Assume.assumeTrue("Backup Service must be enabled", backupProperties.isBackupEnabled());
        Assume.assumeTrue("Cache size is specified", backupProperties.getMaxCacheSize() > 100);
    }

    @Test
    public void testSimpleBackupIntegration() throws IOException {
        GZIPOutputStream outputStream = backupService.getBackupStream("simple-test", "db0", "rp0");
        outputStream.write("test".getBytes());
        outputStream.close();
    }

    @Test
    public void testBackupIntegration() throws IOException {
        String value = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
        Assume.assumeTrue("GOOGLE_APPLICATION_CREDENTIALS environment variable must be configured " +
                        "to point to a credentials file " +
                        "for a Google service account, but is " + value,
                        value != null);

        backupService.writeToBackup("testPayload11 1557777267", new URL("https://test-influx.com:8080"),
                "test-db", "test-policy");

        backupService.flush();
    }
}
