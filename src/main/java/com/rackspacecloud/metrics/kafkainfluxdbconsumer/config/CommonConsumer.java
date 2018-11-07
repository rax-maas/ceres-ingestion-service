package com.rackspacecloud.metrics.kafkainfluxdbconsumer.config;

import com.rackspace.maas.model.Metric;
import com.rackspacecloud.metrics.kafkainfluxdbconsumer.serializer.AvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@EnableConfigurationProperties(ConsumerConfigurationProperties.class)
public class CommonConsumer {

    @Autowired
    ConsumerConfigurationProperties configurationProperties;

    Map<String, Object> consumerProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configurationProperties.getServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, configurationProperties.getConsumer().getGroup());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, configurationProperties.getSessionTimeoutMsConfig());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroDeserializer.class);

        return props;
    }

    @Bean
    ConsumerFactory<String, Metric> consumerFactory(){
        return new DefaultKafkaConsumerFactory<>(consumerProperties(),
                new StringDeserializer(),
                new AvroDeserializer<>(Metric.class));
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, Metric> kafkaListenerContainerFactory(){
        ConcurrentKafkaListenerContainerFactory<String, Metric> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());

        populateContainerProperties(factory);

        return factory;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, Metric> batchFactory(){
        ConcurrentKafkaListenerContainerFactory<String, Metric> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        factory.setBatchListener(true);

        populateContainerProperties(factory);

        return factory;
    }

    private void populateContainerProperties(ConcurrentKafkaListenerContainerFactory<String, Metric> factory) {
        ContainerProperties containerProperties = factory.getContainerProperties();

        containerProperties.setIdleEventInterval(configurationProperties.getListenerContainerIdleInterval());
        containerProperties.setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL);
    }
}
