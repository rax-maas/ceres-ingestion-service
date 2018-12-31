package com.rackspacecloud.metrics.kafkainfluxdbconsumer.config;

import com.rackspacecloud.metrics.kafkainfluxdbconsumer.serializer.AvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Purpose of this class is to create the consumer configuration.
 */
public class ConsumerProperties {
    Map<String, Object> properties;
    ConsumerConfigurationProperties configurationProperties;

    public ConsumerProperties(ConsumerConfigurationProperties configurationProperties){
        this.properties = new HashMap<>();
        this.configurationProperties = configurationProperties;

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configurationProperties.getServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, configurationProperties.getConsumer().getGroup());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
                configurationProperties.getSessionTimeoutMsConfig());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroDeserializer.class);
    }

    public void addSslConfig(){
        properties.put("ssl.keystore.location", configurationProperties.getSsl().getKeystoreLocation());
        properties.put("ssl.keystore.password", configurationProperties.getSsl().getKeystorePassword());
        properties.put("ssl.truststore.location", configurationProperties.getSsl().getTruststoreLocation());
        properties.put("ssl.truststore.password", configurationProperties.getSsl().getTruststorePassword());
        properties.put("ssl.key.password", configurationProperties.getSsl().getKeyPassword());
        properties.put("security.protocol", configurationProperties.getProperties().getSecurityProtocol());
    }
}
