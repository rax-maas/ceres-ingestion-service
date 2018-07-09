package com.rackspacecloud.metrics.kafkainfluxdbconsumer.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties("rest-template")
public class RestTemplateConfigurationProperties {
    RequestConfiguration requestConfig;
    PoolingConnectionManager poolingHttpClientConnectionManager;
    Credential credential;
    long secondsToTokenExpirationTime;

    @Data
    public static class RequestConfiguration {
        int connectionRequestTimeout;
        int connectTimeout;
        int socketTimeout;
    }

    @Data
    public static class PoolingConnectionManager {
        int maxTotal;
    }

    @Data
    public static class Credential {
        String user;
        String apiKey;
        String identityUrl;
    }
}
