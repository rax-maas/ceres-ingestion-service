package com.rackspacecloud.metrics.kafkainfluxdbconsumer.config;

import com.rackspace.maas.model.Metric;
import com.rackspacecloud.metrics.kafkainfluxdbconsumer.serializer.AvroDeserializer;
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
public class Consumer {

    ConsumerConfigurationProperties properties;

    @Autowired
    public Consumer(ConsumerConfigurationProperties properties){
        this.properties = properties;
    }

    /**
     * Create ConsumerProperties bean for dev environment
     * @return
     */
    @Bean
    @Profile("development")
    ConsumerProperties devConsumerProperties() {
        ConsumerProperties consumerProperties = new ConsumerProperties(properties);
//        consumerProperties.addSslConfig();
        return consumerProperties;
    }

    /**
     * Create ConsumerProperties bean for production environment
     * @return
     */
    @Bean
    @Profile("production")
    ConsumerProperties prodConsumerProperties() {
        ConsumerProperties consumerProperties = new ConsumerProperties(properties);
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

        populateContainerProperties(factory, config.configurationProperties);

        return factory;
    }

    private void populateContainerProperties(
            ConcurrentKafkaListenerContainerFactory<String, Metric> factory,
            ConsumerConfigurationProperties configurationProperties) {

        ContainerProperties containerProperties = factory.getContainerProperties();

        containerProperties.setIdleEventInterval(configurationProperties.getListenerContainerIdleInterval());
        containerProperties.setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL);
    }
}
