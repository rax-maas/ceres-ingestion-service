package com.rackspacecloud.metrics.ingestionservice.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;

@Configuration
@ConfigurationProperties("kafka")
@Data
public class ConsumerConfigurationProperties {
    private List<String> servers;
    private long listenerContainerIdleInterval;
    private int sessionTimeoutMsConfig;
    private int heartbeatIntervalMsConfig;
    private int maxPollRecordsConfig;
    private String fetchMinBytesConfig;
    private int fetchMaxWaitMsConfig;
    private String maxPartitionFetchBytesConfig;
    private Properties properties;
    private Ssl ssl;
    private Consumer consumer;

    public void setServers(String servers){
        this.servers = Arrays.asList(servers.split(";"));
    }

    @Data
    public static class Consumer{
        private String group;
    }

    @Data
    public static class Ssl {
        private String truststoreLocation;
        private String truststorePassword;
        private String keystoreLocation;
        private String keystorePassword;
        private String keyPassword;
    }

    @Data
    public static class Properties{
        private String securityProtocol;
    }
}
