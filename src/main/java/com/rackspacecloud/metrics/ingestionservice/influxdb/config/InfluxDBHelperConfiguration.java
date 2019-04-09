package com.rackspacecloud.metrics.ingestionservice.influxdb.config;

import com.rackspacecloud.metrics.ingestionservice.influxdb.InfluxDBHelper;
import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.DevTestTenantRouteProvider;
import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.ProdTenantRouteProvider;
import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.RouteProvider;
import com.rackspacecloud.metrics.ingestionservice.utils.InfluxDBUtils;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;

@Configuration
@EnableConfigurationProperties(RestTemplateConfigurationProperties.class)
public class InfluxDBHelperConfiguration {
    @Value("${influxdb.number-of-points-in-a-write-batch}")
    private int numberOfPointsInAWriteBatch;

    @Value("${influxdb.write-flush-duration-ms-limit}")
    private int writeFlushDurationMsLimit;

    @Autowired
    RestTemplateConfigurationProperties config;

    @Bean
    public PoolingHttpClientConnectionManager poolingHttpClientConnectionManager() {
        PoolingHttpClientConnectionManager poolingConnectionManager = new PoolingHttpClientConnectionManager();
        poolingConnectionManager.setMaxTotal(config.getPoolingHttpClientConnectionManager().getMaxTotal());
        return poolingConnectionManager;
    }

    @Bean
    public RequestConfig requestConfig() {
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(config.getRequestConfig().getConnectionRequestTimeout())
                .setConnectTimeout(config.getRequestConfig().getConnectTimeout())
                .setSocketTimeout(config.getRequestConfig().getSocketTimeout())
                .build();
        return requestConfig;
    }

    @Bean
    public CloseableHttpClient httpClient(
            PoolingHttpClientConnectionManager poolingHttpClientConnectionManager,
            RequestConfig requestConfig) {

        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));

        CloseableHttpClient builder = HttpClientBuilder
                .create()
                .setConnectionManager(poolingHttpClientConnectionManager)
                .setDefaultRequestConfig(requestConfig)
                .build();
        return builder;
    }

    @Bean
    public RestTemplate restTemplate(HttpClient httpClient) {
        HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();
        requestFactory.setHttpClient(httpClient);
        return new RestTemplate(requestFactory);
    }

    @Bean(name = "routeProvider")
    @Profile({"development", "test"})
    public RouteProvider devTestTenantRouteProvider() {
        return new DevTestTenantRouteProvider();
    }

    @Bean(name = "routeProvider")
    @Profile("production")
    public RouteProvider prodTenantRouteProvider() {
        return new ProdTenantRouteProvider();
    }

    @Bean
    public InfluxDBUtils influxDBUtils() {
        return new InfluxDBUtils();
    }

    @Bean
    @Autowired
    public InfluxDBHelper influxDBHelper(
            RestTemplate restTemplate,
            RouteProvider routeProvider,
            MeterRegistry registry,
            InfluxDBUtils influxDBUtils) {
        return new InfluxDBHelper(
                restTemplate,
                routeProvider,
                registry,
                influxDBUtils,
                numberOfPointsInAWriteBatch,
                writeFlushDurationMsLimit);
    }
}
