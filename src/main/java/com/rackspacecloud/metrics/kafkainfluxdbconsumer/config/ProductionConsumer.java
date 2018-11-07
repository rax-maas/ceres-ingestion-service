package com.rackspacecloud.metrics.kafkainfluxdbconsumer.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.Map;

@Configuration
@Profile("production")
public class ProductionConsumer extends CommonConsumer {

    @Override
    Map<String, Object> consumerProperties() {
        Map<String, Object> props = super.consumerProperties();

        props.put("ssl.keystore.location", configurationProperties.getSsl().getKeystoreLocation());
        props.put("ssl.keystore.password", configurationProperties.getSsl().getKeystorePassword());
        props.put("ssl.truststore.location", configurationProperties.getSsl().getTruststoreLocation());
        props.put("ssl.truststore.password", configurationProperties.getSsl().getTruststorePassword());
        props.put("ssl.key.password", configurationProperties.getSsl().getKeyPassword());
        props.put("security.protocol", configurationProperties.getProperties().getSecurityProtocol());

        return props;
    }
}
