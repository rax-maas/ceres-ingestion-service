package com.rackspacecloud.metrics.ingestionservice;

import com.rackspacecloud.metrics.ingestionservice.influxdb.InfluxDBHelper;
import com.rackspacecloud.metrics.ingestionservice.producer.MockMetricHelper;
import com.rackspacecloud.metrics.ingestionservice.producer.Sender;
import com.rackspacecloud.metrics.ingestionservice.listeners.rawlisteners.RawListener;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import org.springframework.boot.test.mock.mockito.*;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {IngestionServiceApplicationTests.UNIFIED_METRICS_TOPIC})
public class IngestionServiceApplicationTests {

	static final String UNIFIED_METRICS_TOPIC = "unified.metrics.json";

	@Autowired
    private RawListener rawListener;

	@Autowired
    private Sender sender;

	@MockBean
    private InfluxDBHelper influxDBHelperMock;

	@Before
    public void setup(){
	    // Nothing to setup at this time.
    }

	@Test
	public void contextLoads() {
	}

	@Test
    public void testConsume() throws Exception {
        // Mock influxDB ingestion call
        when(this.influxDBHelperMock.ingestToInfluxDb(anyString(), anyString(), anyString())).thenReturn(true);

	    for(int i = 0; i < 1; i++) {
            sender.send(
                    MockMetricHelper.getValidMetric(i, "hybrid:1667601", true),
                    UNIFIED_METRICS_TOPIC);
        }

        Thread.sleep(10*1000L); // wait for a few sec for consumer to process some records

        long batchProcessed = rawListener.getBatchProcessedCount();
        Assert.assertTrue(batchProcessed > 0);
    }
}
