package com.rackspacecloud.metrics.ingestionservice;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.rackspacecloud.metrics.ingestionservice.influxdb.GCLineProtocolBackupService;
import com.rackspacecloud.metrics.ingestionservice.influxdb.LocalUUID;
import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.LineProtocolBackupService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.internal.util.reflection.Whitebox;
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
@ActiveProfiles(value = { "raw-data-consumer", "test" })
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = { IngestionServiceApplicationTests.UNIFIED_METRICS_TOPIC })
public class GCLineProtocolBackupServiceTests {

    @Autowired
    private LineProtocolBackupService backupService;

    private GCLineProtocolBackupService gcLineProtocolBackupService;

    @Before
    public void setup() {
        gcLineProtocolBackupService = new GCLineProtocolBackupService();
    }

    @Test
    public void backupServiceGetProperName() {
        LocalUUID mockedUUIDGenerator = mock(LocalUUID.class);
        // TODO: switch to bean/autowire and use profile
        Whitebox.setInternalState(gcLineProtocolBackupService, "uuidGenerator", mockedUUIDGenerator);

        when(mockedUUIDGenerator.generateUUID()).thenReturn(UUID.fromString("90f65f79-f3fc-4eb4-ab5b-f003fbdbe54e"));

        assertThat(GCLineProtocolBackupService.getBackupFilename("testPayload 1557777267", "db1.ceres.google.com",
                "myDB", "1440h"))
                        .isEqualTo("db1.ceres.google.com/myDB/1440h/20190513/90f65f79-f3fc-4eb4-ab5b-f003fbdbe54e.gz");
    }

    @Test
    public void backupServiceGetCachedStream() throws IOException {
        GZIPOutputStream outputStream1 = gcLineProtocolBackupService.getBackupStream("testFile1");
        GZIPOutputStream outputStream2 = gcLineProtocolBackupService.getBackupStream("testFile1");
        assertThat(outputStream1).isEqualTo(outputStream2);
    }
}
