package com.rackspacecloud.metrics.ingestionservice;

import com.rackspacecloud.metrics.ingestionservice.influxdb.GCLineProtocolBackupService;
import com.rackspacecloud.metrics.ingestionservice.influxdb.LocalUUID;
import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.LineProtocolBackupService;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.internal.util.reflection.Whitebox;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles(value = { "test" })
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = { IngestionServiceApplicationTests.UNIFIED_METRICS_TOPIC })
public class GCLineProtocolBackupServiceTests {

    @Autowired
    private LineProtocolBackupService backupService;

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
    public void checkFiles() throws IOException {
        AopUtils.
        Object object = Whitebox.getInternalState(backupService, "proxy");
        GZIPOutputStream outputStream1 = backupService.getBackupStream("testFile1");
        GZIPOutputStream outputStream2 = backupService.getBackupStream("testFile1");
        GZIPOutputStream outputStream3 = backupService.getBackupStream("testFile2");
        assertThat(outputStream1).isEqualTo(outputStream2);
        assertThat(outputStream2).isNotEqualTo(outputStream3);
    }
}
