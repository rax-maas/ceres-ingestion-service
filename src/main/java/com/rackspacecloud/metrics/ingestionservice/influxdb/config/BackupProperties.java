package com.rackspacecloud.metrics.ingestionservice.influxdb.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.Duration;

@Component
@Data
@ConfigurationProperties("backup")
public class BackupProperties {
    @NotBlank(message = "gcs-backup-bucket should be configured, but is not")
    private String gcsBackupBucket;
    @NotNull(message = "gcs-timeout duration should be configured, but is not")
    private Duration gcsTimeout;
    // This clears the entire cache on a timer    
    private long gcsFlushMilliseconds;
    // Force flush stream after each write if true    
    private boolean alwaysFlush;
}
