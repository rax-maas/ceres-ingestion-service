package com.rackspacecloud.metrics.ingestionservice.influxdb.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.rackspacecloud.metrics.ingestionservice.influxdb.GCLineProtocolBackupService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableCaching
public class BackupCacheConfiguration {

    @Autowired
    private BackupProperties backupProperties;

    // Configure cache for "lineProtocolBackupWriter" cache
    @Bean
    public CacheManager cacheManager() {
        CaffeineCacheManager ccm = new CaffeineCacheManager("lineProtocolBackupWriter");
        ccm.setCaffeine(
                Caffeine.newBuilder()
                        .expireAfterAccess(backupProperties.getGcsTimeout())
                        .removalListener(GCLineProtocolBackupService.removalListener)
                        );
        return ccm;
    }
}
