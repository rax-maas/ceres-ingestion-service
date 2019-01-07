package com.rackspacecloud.metrics.ingestionservice.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Purpose of this class is to create common configuration for consumers.
 */
public abstract class ConsumerProperties {
    public Map<String, Object> properties;
    public ConsumerConfigurationProperties configurationProperties;

    public ConsumerProperties(ConsumerConfigurationProperties configProps){
        this.properties = new HashMap<>();
        this.configurationProperties = configProps;

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configProps.getServers());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, configProps.getSessionTimeoutMsConfig());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
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
