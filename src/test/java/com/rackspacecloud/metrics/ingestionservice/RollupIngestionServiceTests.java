package com.rackspacecloud.metrics.ingestionservice;

import com.rackspacecloud.metrics.ingestionservice.influxdb.InfluxDBHelper;
import com.rackspacecloud.metrics.ingestionservice.listeners.rolluplisteners.RollupListener;
import com.rackspacecloud.metrics.ingestionservice.producer.MockMetricRollupHelper;
import com.rackspacecloud.metrics.ingestionservice.producer.Sender;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles(value = {"rollup-data-consumer","test"})
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {
        RollupIngestionServiceTests.UNIFIED_METRICS_5M_TOPIC,
        RollupIngestionServiceTests.UNIFIED_METRICS_20M_TOPIC,
        RollupIngestionServiceTests.UNIFIED_METRICS_60M_TOPIC,
        RollupIngestionServiceTests.UNIFIED_METRICS_240M_TOPIC,
        RollupIngestionServiceTests.UNIFIED_METRICS_1440M_TOPIC
})
public class RollupIngestionServiceTests {
    static final String UNIFIED_METRICS_5M_TOPIC = "unified.metrics.json.5m";
    static final String UNIFIED_METRICS_20M_TOPIC = "unified.metrics.json.20m";
    static final String UNIFIED_METRICS_60M_TOPIC = "unified.metrics.json.60m";
    static final String UNIFIED_METRICS_240M_TOPIC = "unified.metrics.json.240m";
    static final String UNIFIED_METRICS_1440M_TOPIC = "unified.metrics.json.1440m";

    @Autowired
    private RollupListener rollupListener;

	@Autowired
    private Sender sender;

	@MockBean
    private InfluxDBHelper influxDBHelperMock;

//	@Autowired
//    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

//    @Before
//    public void setUp() throws Exception {
//        // wait until the partitions are assigned
//        for (MessageListenerContainer listenerContainer : kafkaListenerEndpointRegistry.getListenerContainers()) {
//            ContainerTestUtils.waitForAssignment(listenerContainer,1);
//        }
//    }

//    @Test
//    public void test_Rollup5M_Consume() throws Exception {
//        String topicToSendData = UNIFIED_METRICS_5M_TOPIC;
//        testForGivenTopic(topicToSendData);
//    }
//
//    @Test
//    public void test_Rollup20M_Consume() throws Exception {
//        String topicToSendData = UNIFIED_METRICS_20M_TOPIC;
//        testForGivenTopic(topicToSendData);
//    }
//
//    @Test
//    public void test_Rollup60M_Consume() throws Exception {
//        String topicToSendData = UNIFIED_METRICS_60M_TOPIC;
//        testForGivenTopic(topicToSendData);
//    }
//
//    @Test
//    public void test_Rollup240M_Consume() throws Exception {
//        String topicToSendData = UNIFIED_METRICS_240M_TOPIC;
//        testForGivenTopic(topicToSendData);
//    }
//
//    @Test
//    public void test_Rollup1440M_Consume() throws Exception {
//        String topicToSendData = UNIFIED_METRICS_1440M_TOPIC;
//        testForGivenTopic(topicToSendData);
//    }
//
//    private void testForGivenTopic(String topicToSendData) throws Exception {
//        // Mock influxDB ingestion call
//        when(this.influxDBHelperMock.ingestToInfluxDb(anyString(), anyString(), anyString(), anyString()))
//                .thenReturn(true);
//
//        for(int i = 0; i < 1; i++) {
//            sender.sendRollup(
//                    MockMetricRollupHelper.getValidMetricRollup(i, "hybrid:1667601", true),
//                    topicToSendData);
//        }
//
//        Thread.sleep(10*1000L); // wait for a few sec for consumer to process some records
//
//        long batchProcessed = rollupListener.getBatchProcessedCount();
//        Assert.assertTrue(
//                String.format("Failed with batchProcessed count [%s]", batchProcessed), batchProcessed > 0);
//    }
}
