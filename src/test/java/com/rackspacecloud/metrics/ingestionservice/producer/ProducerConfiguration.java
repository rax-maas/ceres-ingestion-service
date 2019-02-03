package com.rackspacecloud.metrics.ingestionservice.producer;

import com.rackspace.maas.model.Metric;
import com.rackspacecloud.metrics.ingestionservice.listeners.rolluplisteners.models.MetricRollup;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@Profile("test")
public class ProducerConfiguration {

    @Value("${kafka.servers}")
    private String kafkaServers;

    @Bean
    public Map<String, Object> rawProducerConfig(){
        Map<String, Object> props = getProperties();
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class);

        return props;
    }

    @Bean
    public ProducerFactory<String, Metric> rawProducerFactory() {
        return new DefaultKafkaProducerFactory<>(rawProducerConfig());
    }

    @Bean
    public KafkaTemplate<String, Metric> rawKafkaTemplate() {
        return new KafkaTemplate<>(rawProducerFactory());
    }

    @Bean
    public Map<String, Object> rollupProducerConfig(){
        Map<String, Object> props = getProperties();
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RollupSerializer.class);

        return props;
    }

    @Bean
    public ProducerFactory<String, MetricRollup> rollupProducerFactory() {
        return new DefaultKafkaProducerFactory<>(rollupProducerConfig());
    }

    @Bean
    public KafkaTemplate<String, MetricRollup> rollupKafkaTemplate() {
        return new KafkaTemplate<>(rollupProducerFactory());
    }

    private Map<String, Object> getProperties() {
        Map<String, Object> props = new HashMap<>();

        List<String> servers = new ArrayList<>();
        servers.add(kafkaServers);

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    @Bean
    public Sender sender(){
        return new Sender();
    }
}
