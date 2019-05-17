package com.rackspacecloud.metrics.ingestionservice;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.rackspacecloud.metrics.ingestionservice.influxdb.GCLineProtocolBackupService;
import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.LineProtocolBackupService;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.nio.channels.Channels;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles(value = { "test" })
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = { IngestionServiceApplicationTests.UNIFIED_METRICS_TOPIC })
public class GCLineProtocolBackupServiceTests {

    @Autowired
    private LineProtocolBackupService backupService;

    @Autowired
    private Storage storage;

    @Value("${backup.gcs-backup-bucket}")
    private String bucket;

    @Test
    public void backupServiceGetProperName() {
        assertThat(GCLineProtocolBackupService.getBackupFilename("testPayload 1557777267", "db1.ceres.google.com",
                "myDB", "1440h"))
                        .matches("db1\\.ceres\\.google\\.com/myDB/1440h/20190513/.*\\.gz");
    }

    @Test
    public void backupServiceGetCachedStream() throws IOException {
        GZIPOutputStream outputStream1 = backupService.getBackupStream("testFile1");
        GZIPOutputStream outputStream2 = backupService.getBackupStream("testFile1");
        GZIPOutputStream outputStream3 = backupService.getBackupStream("testFile1");
        assertThat(outputStream1).isEqualTo(outputStream2);
        assertThat(outputStream2).isEqualTo(outputStream3);
    }

    @Test
    public void backupServiceGetCachedStream2() throws IOException {
        GZIPOutputStream outputStream1 = backupService.getBackupStream("testFile1");
        GZIPOutputStream outputStream2 = backupService.getBackupStream("testFile1");
        GZIPOutputStream outputStream3 = backupService.getBackupStream("testFile2");
        assertThat(outputStream1).isEqualTo(outputStream2);
        assertThat(outputStream2).isNotEqualTo(outputStream3);
    }

    @Test
    public void checkFile() throws IOException {
        GZIPOutputStream outputStream1 = backupService.getBackupStream("testFile1");
        outputStream1.write("test1".getBytes());
        outputStream1.close();
        assertThat(IOUtils.toString(new GZIPInputStream(Channels.newInputStream(storage.reader(bucket, "testFile1"))))).isEqualTo("test1");
    }

    @Test
    public void checkBucket() {
        assertThat(bucket).isEqualTo("ceres-backup-dev");
    }

    @Test(expected = StorageException.class)
    public void testTimeoutNoTimeout() throws IOException {
        GZIPOutputStream outputStream1 = backupService.getBackupStream("testFile1");
        outputStream1.write("test1".getBytes());
        IOUtils.toString(new GZIPInputStream(Channels.newInputStream(storage.reader(bucket, "testFile1"))));
    }

    @Test
    public void testTimeoutWithTimeout() throws IOException, InterruptedException {
        GZIPOutputStream outputStream1 = backupService.getBackupStream("testFile1");
        outputStream1.write("test1".getBytes());
        TimeUnit.SECONDS.sleep(10);
        assertThat(IOUtils.toString(new GZIPInputStream(Channels.newInputStream(storage.reader(bucket, "testFile1"))))).isEqualTo("test1");
    }

    @Test
    public void testServiceTwoDbInstances() throws IOException {
        backupService.writeToBackup("testPayload1 1557777267", "db1.ceres.google.com",
                "myDB", "1440h");
        backupService.writeToBackup("testPayload2 1557777268", "db1.ceres.google.com",
                "myDB", "1440h");
        backupService.writeToBackup("testPayload3 1557777269", "db1.ceres.google.com",
                "myDB", "1440h");

        backupService.writeToBackup("testPayload1 1557777267", "db2.ceres.google.com",
                "myDB", "1440h");
        backupService.writeToBackup("testPayload2 1557777268", "db2.ceres.google.com",
                "myDB", "1440h");
        backupService.writeToBackup("testPayload3 1557777269", "db2.ceres.google.com",
                "myDB", "1440h");


    }
}
