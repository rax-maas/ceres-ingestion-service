package com.rackspacecloud.metrics.ingestionservice.influxdb.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.rackspacecloud.metrics.ingestionservice.influxdb.LineProtocolBackupService;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
@EnableCaching
public class BackupCacheConfiguration {
    @Bean
    public LineProtocolBackupService lineProtocolBackupService() {
        return new LineProtocolBackupService();
    }

    // Configure cache for "backupBuffer" cache
    // TODO rename to backupBuffer to something better
    @Bean
    public CacheManager cacheManager() {
        CaffeineCacheManager ccm = new CaffeineCacheManager("backupBuffer");
        ccm.setCaffeine(
                Caffeine.newBuilder()
                        .expireAfterAccess(Duration.ofHours(1))
                        .removalListener(LineProtocolBackupService.removalListener)
                        );
        return ccm;
    }
}
