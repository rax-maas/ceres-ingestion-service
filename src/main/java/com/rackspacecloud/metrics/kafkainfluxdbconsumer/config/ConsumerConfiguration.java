package com.rackspacecloud.metrics.kafkainfluxdbconsumer.config;

import com.rackspace.maas.model.Metric;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;

@Configuration
@EnableKafka
@EnableConfigurationProperties(ConsumerConfigurationProperties.class)
public class ConsumerConfiguration {

    @Autowired
    ConsumerFactory<String, Metric> consumerFactory;

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, Metric> kafkaListenerContainerFactory(){
        ConcurrentKafkaListenerContainerFactory<String, Metric> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL);

        return factory;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, Metric> batchFactory(){
        ConcurrentKafkaListenerContainerFactory<String, Metric> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL);

        return factory;
    }
}
