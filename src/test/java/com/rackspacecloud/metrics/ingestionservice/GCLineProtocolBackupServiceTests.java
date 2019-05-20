package com.rackspacecloud.metrics.ingestionservice;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.rackspacecloud.metrics.ingestionservice.influxdb.GCLineProtocolBackupService;
import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.LineProtocolBackupService;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

    @Before
    public void setUp() throws InterruptedException {
        // Purge any remaining buffers
        backupService.clear();
        // Make sure cache is purged
        
        // Clear all storage
        if(storage.list(bucket)!=null) {
            storage.list(bucket).iterateAll().forEach(blob -> blob.delete());
        }
    }

    @Test
    public void backupServiceGetProperName() {
        assertThat(GCLineProtocolBackupService.getBackupLocation("testPayload 1557777267", "db1.ceres.google.com",
                "myDB", "1440h"))
                        .matches("db1\\.ceres\\.google\\.com/myDB/1440h/20190513");
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
        assertThat(IOUtils.toString(new GZIPInputStream(Channels.newInputStream(storage.reader(bucket,
                storage.list(bucket).getValues().iterator().next().getName()))))).isEqualTo("test1");
    }

    @Test
    public void checkBucket() {
        assertThat(bucket).isEqualTo("ceres-backup-dev");
    }

    @Test(expected = StorageException.class)
    public void testWriteAndRead() throws IOException {
        GZIPOutputStream outputStream1 = backupService.getBackupStream("testFile1");
        outputStream1.write("test1".getBytes());
        IOUtils.toString(new GZIPInputStream(Channels.newInputStream(storage.reader(bucket,
                storage.list(bucket).getValues().iterator().next().getName()))));
    }

    @Test
    public void testCacheClear() throws IOException, InterruptedException {
        GZIPOutputStream outputStream1 = backupService.getBackupStream("testFile1");
        outputStream1.write("test1".getBytes());
        backupService.clear();
        
        assertThat(IOUtils.toString(new GZIPInputStream(Channels.newInputStream(storage.reader(bucket,
                storage.list(bucket).getValues().iterator().next().getName()))))).isEqualTo("test1");
    }

    @Test
    public void testServiceTwoDbInstances() throws IOException, InterruptedException {
        backupService.writeToBackup("testPayload11 1557777267", "db1.ceres.google.com",
                "myDB", "1440h");
        backupService.writeToBackup("testPayload12 1557777268", "db1.ceres.google.com",
                "myDB", "1440h");
        backupService.writeToBackup("testPayload13 1557777269", "db1.ceres.google.com",
                "myDB", "1440h");

        backupService.writeToBackup("testPayload21 1557777267", "db2.ceres.google.com",
                "myDB", "1440h");
        backupService.writeToBackup("testPayload22 1557777268", "db2.ceres.google.com",
                "myDB", "1440h");
        backupService.writeToBackup("testPayload23 1557777269", "db2.ceres.google.com",
                "myDB", "1440h");

        backupService.clear();
        

        Iterator<Blob> iterator = storage.list(bucket).getValues().iterator();

        List<Blob> blobList = new ArrayList<>();
        iterator.forEachRemaining(blobList::add);

        assertThat(blobList.size()).isEqualTo(2);

        String blob1 = blobList.get(0).getName();
        String blob2 = blobList.get(1).getName();
        assertThat(IOUtils.toString(new GZIPInputStream(Channels.newInputStream(storage.reader(bucket, blob1)))))
                .matches("testPayload[12]1 1557777267\ntestPayload[12]2 1557777268\ntestPayload[12]3 1557777269\n");
        assertThat(IOUtils.toString(new GZIPInputStream(Channels.newInputStream(storage.reader(bucket, blob2)))))
                .matches("testPayload[12]1 1557777267\ntestPayload[12]2 1557777268\ntestPayload[12]3 1557777269\n");
    }
}
