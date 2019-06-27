package com.rackspacecloud.metrics.ingestionservice.influxdb.config;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@EnableConfigurationProperties(BackupProperties.class)
@Slf4j
public class StorageConfiguration {

    @Bean
    @Profile({"production"})
    public Storage gcStorage(){
        // This should grab the file pointed to by the GOOGLE_APPLICATION_CREDENTIALS environment variable
        log.debug("Configuring google cloud storage: {}", StorageOptions.getDefaultInstance().getService());
        return StorageOptions.getDefaultInstance().getService();
    }

    @Bean
    @Profile({"test", "development"})
    public Storage memStorage(){
        // In-memory storage for testing
        log.info("Configuring local storage for testing");
        return LocalStorageHelper.getOptions().getService();
    }
}
