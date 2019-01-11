package com.rackspacecloud.metrics.ingestionservice.listeners.rawlisteners.config;

import com.rackspacecloud.metrics.ingestionservice.config.ConsumerConfigurationProperties;
import com.rackspacecloud.metrics.ingestionservice.config.ConsumerProperties;
import com.rackspacecloud.metrics.ingestionservice.listeners.rawlisteners.deserializer.AvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.context.annotation.Profile;

@Profile("raw-data-consumer")
public class RawDataConsumerProperties extends ConsumerProperties {
    private static final String CONSUMER_GROUP_SUFFIX = "-raw";

    public RawDataConsumerProperties(ConsumerConfigurationProperties configProps) {
        super(configProps);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, configProps.getConsumer().getGroup() + CONSUMER_GROUP_SUFFIX);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroDeserializer.class);
    }
}
