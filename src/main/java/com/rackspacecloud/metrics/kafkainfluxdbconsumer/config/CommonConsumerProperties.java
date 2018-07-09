package com.rackspacecloud.metrics.kafkainfluxdbconsumer.config;

import com.rackspace.maas.model.Metrics;
import com.rackspacecloud.metrics.kafkainfluxdbconsumer.serializer.AvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class CommonConsumerProperties {

    @Autowired
    ConsumerConfigurationProperties configurationProperties;

    Map<String, Object> consumerProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configurationProperties.getServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, configurationProperties.getConsumer().getGroup());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroDeserializer.class);

        return props;
    }

    @Bean
    ConsumerFactory<String, Metrics> consumerFactory(){
        return new DefaultKafkaConsumerFactory<>(consumerProperties(),
                new StringDeserializer(),
                new AvroDeserializer<>(Metrics.class));
    }
}
