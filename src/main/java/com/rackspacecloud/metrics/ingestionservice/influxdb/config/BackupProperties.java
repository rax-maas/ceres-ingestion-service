package com.rackspacecloud.metrics.ingestionservice.influxdb.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@Data
@ConfigurationProperties("backup")
public class BackupProperties {
    private String gcsBackupBucket;
    private Duration gcsTimeout;
}
