package com.rackspacecloud.metrics.ingestionservice;

import com.rackspacecloud.metrics.ingestionservice.exceptions.IngestFailedException;
import com.rackspacecloud.metrics.ingestionservice.influxdb.InfluxDBHelper;
import com.rackspacecloud.metrics.ingestionservice.listeners.rawlisteners.RawListener;
import com.rackspacecloud.metrics.ingestionservice.producer.MockMetricHelper;
import com.rackspacecloud.metrics.ingestionservice.producer.Sender;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles(value = {"raw-data-consumer","test"})
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {
        IngestionServiceApplicationTests.UNIFIED_METRICS_TOPIC,
        IngestionServiceApplicationTests.UNIFIED_METRICS_5M_TOPIC
})
public class IngestionServiceApplicationTests {

	static final String UNIFIED_METRICS_TOPIC = "unified.metrics.json";
  static final String UNIFIED_METRICS_5M_TOPIC = "unified.metrics.json.5m";

	@Autowired
  private RawListener rawListener;

	@Autowired
  private Sender sender;

	@MockBean
  private InfluxDBHelper influxDBHelperMock;

  @Test
  @Ignore
  public void testSuccessfulRawDataConsumption()
      throws InterruptedException, IngestFailedException {
      // Mock influxDB ingestion call
    doNothing().when(this.influxDBHelperMock).ingestToInfluxDb(anyString(), anyString(), anyString(), anyString());

    for(int i = 0; i < 1; i++) {
      sender.send(MockMetricHelper.getValidMetric(
            i,
            "CORE",
            "hybrid:1667601",
            13,
            true),
          UNIFIED_METRICS_TOPIC);
    }

      Thread.sleep(10*1000L); // wait for a few sec for consumer to process some records

      long batchProcessed = rawListener.getBatchProcessedCount();
      Assert.assertTrue(batchProcessed > 0);
  }

  @Test
  @Ignore
  public void test_whenIngestToInfluxDBThrowsException_globalExceptionHandlerCatches()
      throws InterruptedException, IngestFailedException {
      // Mock influxDB ingestion call
      doThrow(new IngestFailedException("test_whenIngestToInfluxDBThrowsException_globalExceptionHandlerCatches"))
      .when(this.influxDBHelperMock).ingestToInfluxDb(anyString(), anyString(), anyString(), anyString());

      for(int i = 0; i < 1; i++) {
          sender.send(
                  MockMetricHelper.getValidMetric(i, "CORE", "hybrid:1667601",
                          11, true),
                  UNIFIED_METRICS_TOPIC);
      }

      Thread.sleep(10*1000L); // wait for a few sec for consumer to process some records

      // Batch processed count will still be more than 0 because exception thrown doesn't
      // mean that batch is not processed
      long batchProcessed = rawListener.getBatchProcessedCount();

      Assert.assertTrue(batchProcessed > 0);
  }
}
