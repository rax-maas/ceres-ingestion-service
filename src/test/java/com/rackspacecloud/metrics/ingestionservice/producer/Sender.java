package com.rackspacecloud.metrics.ingestionservice.producer;

import com.rackspace.monplat.protocol.ExternalMetric;
import com.rackspacecloud.metrics.ingestionservice.listeners.rolluplisteners.models.MetricRollup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

public class Sender {
    private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

    @Autowired
    private KafkaTemplate<String, ExternalMetric> kafkaTemplate;

    @Autowired
    private KafkaTemplate<String, MetricRollup> kafkaTemplateRollup;

    public void send(ExternalMetric payload, String topic) {
        LOGGER.info("START: Sending payload [{}]", payload);
        kafkaTemplate.send(topic, payload);
        LOGGER.info("FINISH: Processing");
    }

    public void sendRollup(MetricRollup payload, String topic) {
        LOGGER.info("START: Sending payload [{}]", payload);
        kafkaTemplateRollup.send(topic, payload);
        LOGGER.info("FINISH: Processing");
    }
}