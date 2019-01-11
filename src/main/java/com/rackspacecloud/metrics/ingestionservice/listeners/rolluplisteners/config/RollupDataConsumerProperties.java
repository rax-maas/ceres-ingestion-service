package com.rackspacecloud.metrics.ingestionservice.listeners.rolluplisteners.config;

import com.rackspacecloud.metrics.ingestionservice.config.ConsumerConfigurationProperties;
import com.rackspacecloud.metrics.ingestionservice.config.ConsumerProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.support.serializer.JsonDeserializer;

@Profile("rollup-data-consumer")
public class RollupDataConsumerProperties extends ConsumerProperties {
    private static final String CONSUMER_GROUP_SUFFIX = "-rollup";

    public RollupDataConsumerProperties(ConsumerConfigurationProperties configProps) {
        super(configProps);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, configProps.getConsumer().getGroup() + CONSUMER_GROUP_SUFFIX);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
    }
}
