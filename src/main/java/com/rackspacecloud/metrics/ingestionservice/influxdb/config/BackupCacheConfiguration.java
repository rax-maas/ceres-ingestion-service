package com.rackspacecloud.metrics.ingestionservice.influxdb.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.rackspacecloud.metrics.ingestionservice.influxdb.GCLineProtocolBackupService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
@EnableCaching
public class BackupCacheConfiguration {
    @Value("${backup.gcs-timeout}")
    private Duration duration;

    // Configure cache for "lineProtocolBackupWriter" cache
    @Bean
    public CacheManager cacheManager() {
        CaffeineCacheManager ccm = new CaffeineCacheManager("lineProtocolBackupWriter");
        ccm.setCaffeine(
                Caffeine.newBuilder()
                        .expireAfterAccess(duration)
                        .removalListener(GCLineProtocolBackupService.removalListener)
                        );
        return ccm;
    }
}
