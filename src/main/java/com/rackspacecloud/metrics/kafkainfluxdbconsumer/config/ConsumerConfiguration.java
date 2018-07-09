package com.rackspacecloud.metrics.kafkainfluxdbconsumer.config;

import com.rackspace.maas.model.Metrics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

@Configuration
@EnableKafka
@EnableConfigurationProperties(ConsumerConfigurationProperties.class)
public class ConsumerConfiguration {

    @Autowired
    ConsumerFactory<String, Metrics> consumerFactory;

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, Metrics> kafkaListenerContainerFactory(){
        ConcurrentKafkaListenerContainerFactory<String, Metrics> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        return factory;
    }
}
