package com.rackspacecloud.metrics.ingestionservice.listeners.rolluplisteners.config;

import com.rackspacecloud.metrics.ingestionservice.config.ConsumerConfigurationProperties;
import com.rackspacecloud.metrics.ingestionservice.config.ConsumerProperties;
import com.rackspacecloud.metrics.ingestionservice.influxdb.InfluxDBHelper;
import com.rackspacecloud.metrics.ingestionservice.listeners.rolluplisteners.RollupListener;
import com.rackspacecloud.metrics.ingestionservice.listeners.rolluplisteners.models.MetricRollup;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

@Configuration
@Profile("rollup-data-consumer")
public class RollupDataConsumerConfiguration {

    ConsumerConfigurationProperties properties;

    @Autowired
    public RollupDataConsumerConfiguration(ConsumerConfigurationProperties properties){
        this.properties = properties;
    }

    @Bean
    @Profile("development")
    ConsumerProperties devConsumerProperties() {
        ConsumerProperties consumerProperties = new RollupDataConsumerProperties(properties);
//        consumerProperties.addSslConfig();
        return consumerProperties;
    }

    @Bean
    @Profile("test")
    ConsumerProperties testConsumerProperties() {
        ConsumerProperties consumerProperties = new RollupDataConsumerProperties(properties);
        return consumerProperties;
    }

    @Bean
    @Profile("production")
    ConsumerProperties prodConsumerProperties() {
        ConsumerProperties consumerProperties = new RollupDataConsumerProperties(properties);
        consumerProperties.addSslConfig();
        return consumerProperties;
    }

    ConsumerFactory<String, MetricRollup> consumerFactory(ConsumerProperties config){
        return new DefaultKafkaConsumerFactory<>(
                config.properties,
                new StringDeserializer(),
                new JsonDeserializer<>(MetricRollup.class));
    }

    /**
     * Creates ConcurrentKafkaListenerContainerFactory and sets kafka-consumer-factory using given
     * kafka consumer configuration
     * @param config
     * @return
     */
    @Bean
    @Autowired
    ConcurrentKafkaListenerContainerFactory<String, MetricRollup> batchFactory(ConsumerProperties config) {
        ConcurrentKafkaListenerContainerFactory<String, MetricRollup> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory(config));
        factory.setBatchListener(true);

        ContainerProperties containerProperties = factory.getContainerProperties();
        containerProperties.setIdleEventInterval(properties.getListenerContainerIdleInterval());
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL);

        return factory;
    }

    @Bean
    @Autowired
    public RollupListener rollupListener(InfluxDBHelper influxDBHelper, MeterRegistry registry){
        return new RollupListener(influxDBHelper, registry);
    }
}
