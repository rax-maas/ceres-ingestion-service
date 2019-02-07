package com.rackspacecloud.metrics.ingestionservice.config;

import io.micrometer.core.aop.TimedAspect;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageHeaders;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class CommonConsumerConfiguration {
    private static Logger LOGGER = LoggerFactory.getLogger(CommonConsumerConfiguration.class);

    /**
     * This error handler is used by kafka listeners to handle any exception in that listener.
     * @return
     */
    @Bean
    public ConsumerAwareListenerErrorHandler listenerErrorHandler() {
        return (message, e, consumer) -> {
            MessageHeaders headers = message.getHeaders();
            List<String> topics = headers.get(KafkaHeaders.RECEIVED_TOPIC, List.class);
            List<Integer> partitions = headers.get(KafkaHeaders.RECEIVED_PARTITION_ID, List.class);
            List<Long> offsets = headers.get(KafkaHeaders.OFFSET, List.class);

            Map<TopicPartition, Long> offsetsToReset = new HashMap<>();

            for(int i = 0; i < topics.size(); i++){
                int index = i;

                offsetsToReset.compute(new TopicPartition(topics.get(i), partitions.get(i)),
                        (k, v) -> v == null ? offsets.get(index) : Math.min(v, offsets.get(index)));
            }

            List<String> topicPartitions = new ArrayList<>();
            offsetsToReset.forEach((k, v) -> {
                consumer.seek(k, v);
                topicPartitions.add("topic=" + k.topic() + " and partition=" + k.partition());
            });

            String allTopicPartitions = String.join(",", topicPartitions);

            LOGGER.error("Kafka listener failed for these topics and partitions [{}].", allTopicPartitions, e);

            return null;
        };
    }

    @Bean
    public MeterFilter metricsFilter() {
        String denyString = "jvm";
        LOGGER.info("Filtering out string [{}]", denyString);
        return MeterFilter.denyNameStartsWith(denyString);
    }

    @Bean
    @Autowired
    public TimedAspect timedAspect(MeterRegistry registry) {
        return new TimedAspect(registry);
    }
}
