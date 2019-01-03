package com.rackspacecloud.metrics.ingestionservice;

import org.apache.kafka.clients.consumer.Consumer;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
public class UnifiedMetricsListenerTest {
    private static final String TEMPLATE_TOPIC = "unified.metrics.json";

    @ClassRule
    public static KafkaEmbedded kafkaEmbedded = new KafkaEmbedded(1, false, TEMPLATE_TOPIC);

    @BeforeClass
    public static void setup() {
        System.setProperty("spring.kafka.bootstrap-servers", kafkaEmbedded.getBrokersAsString());
    }

    @Test
    public void test() throws Exception {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(kafkaEmbedded);
        ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(producerProps);
        KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);

        template.sendDefault(1, "blah data");

        Map<String, Object> consumerProps =
                KafkaTestUtils.consumerProps("testGroup", "false", kafkaEmbedded);
        DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        Consumer<Integer, String> consumer = cf.createConsumer();

        kafkaEmbedded.consumeFromAllEmbeddedTopics(consumer);
    }
}
