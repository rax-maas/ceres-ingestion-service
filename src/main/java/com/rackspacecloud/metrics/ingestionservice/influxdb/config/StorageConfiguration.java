package com.rackspacecloud.metrics.ingestionservice.influxdb.config;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@EnableConfigurationProperties(BackupProperties.class)
public class StorageConfiguration {

    @Bean
    @Profile("production")
    public Storage gcStorage(){
        // This should grab the file pointed to by the GOOGLE_APPLICATION_CREDENTIALS environment variable
        return StorageOptions.getDefaultInstance().getService();
    }

    @Bean
    @Profile({"test", "dev"})
    public Storage memStorage(){
        // In-memory storage for testing
        return LocalStorageHelper.getOptions().getService();
    }
}
