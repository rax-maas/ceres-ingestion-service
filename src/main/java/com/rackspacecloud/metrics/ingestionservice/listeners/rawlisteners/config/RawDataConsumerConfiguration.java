package com.rackspacecloud.metrics.ingestionservice.listeners.rawlisteners.config;

import com.rackspace.maas.model.Metric;
import com.rackspacecloud.metrics.ingestionservice.config.ConsumerConfigurationProperties;
import com.rackspacecloud.metrics.ingestionservice.config.ConsumerProperties;
import com.rackspacecloud.metrics.ingestionservice.influxdb.InfluxDBHelper;
import com.rackspacecloud.metrics.ingestionservice.listeners.rawlisteners.RawListener;
import com.rackspacecloud.metrics.ingestionservice.listeners.rawlisteners.deserializer.AvroDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;

@Configuration
@EnableKafka
@Profile("raw-data-consumer")
public class RawDataConsumerConfiguration {

    ConsumerConfigurationProperties properties;

    @Autowired
    public RawDataConsumerConfiguration(ConsumerConfigurationProperties properties){
        this.properties = properties;
    }

    /**
     * Create ConsumerProperties bean for dev environment
     * @return
     */
    @Bean
    @Profile("development")
    ConsumerProperties devConsumerProperties() {
        ConsumerProperties consumerProperties = new RawDataConsumerProperties(properties);
//        consumerProperties.addSslConfig();
        return consumerProperties;
    }

    /**
     * Create ConsumerProperties bean for unit test
     * @return
     */
    @Bean
    @Profile("test")
    ConsumerProperties testConsumerProperties() {
        ConsumerProperties consumerProperties = new RawDataConsumerProperties(properties);
        return consumerProperties;
    }

    /**
     * Create ConsumerProperties bean for production environment
     * @return
     */
    @Bean
    @Profile("production")
    ConsumerProperties prodConsumerProperties() {
        ConsumerProperties consumerProperties = new RawDataConsumerProperties(properties);
        consumerProperties.addSslConfig();
        return consumerProperties;
    }

    /**
     * Create ConcurrentKafkaListenerContainerFactory bean to process batched messages
     * @param config
     * @return
     */
    @Bean
    @Autowired
    ConcurrentKafkaListenerContainerFactory<String, Metric> batchFactory(ConsumerProperties config){
        ConcurrentKafkaListenerContainerFactory<String, Metric> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        ConsumerFactory<String, Metric> consumerFactory =
                new DefaultKafkaConsumerFactory<>(config.properties,
                        new StringDeserializer(),
                        new AvroDeserializer<>(Metric.class));

        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);

        ContainerProperties containerProperties = factory.getContainerProperties();
        containerProperties.setIdleEventInterval(config.configurationProperties.getListenerContainerIdleInterval());
        containerProperties.setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL);

        return factory;
    }

    /**
     * Create UnifiedMetricsListener
     * @param influxDBHelper
     * @return
     */
    @Bean
    @Autowired
    public RawListener unifiedMetricsListener(InfluxDBHelper influxDBHelper) {
        return new RawListener(influxDBHelper);
    }
}
