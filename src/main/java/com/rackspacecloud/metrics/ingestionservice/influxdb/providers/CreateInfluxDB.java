package com.rackspacecloud.metrics.ingestionservice.influxdb.providers;

import org.influxdb.InfluxDBFactory;
import com.rackspacecloud.metrics.ingestionservice.influxdb.config.BackupProperties;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.Pong;
import org.influxdb.dto.QueryResult;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.RestoreDBInfluxDB;
import java.io.IOException;
import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.NotcreatedDBInfluxDB;
//import org.springframework.kafka.test.context.EmbeddedKafka;
//@ActiveProfiles(value = { "test" })
//@EmbeddedKafka(partitions = 1, topics = { IngestionServiceApplicationTests.UNIFIED_METRICS_TOPIC })
@EnableConfigurationProperties(BackupProperties.class)
public class CreateInfluxDB {
    // User name
    private String username;
    // Password
    private String password;
    // Connection address
    private String openurl;
    // data base
    private String database;
    // Reservation strategy
    private String retentionPolicy;
    private InfluxDB influxDB;

    public CreateInfluxDB(String username, String password, String openurl, String database,
                          String retentionPolicy) {
        this.username = "Ravi5626";
        this.password = "*****";
        this.openurl = "http://localhost:8086";
        this.database = "db_1";
        this.retentionPolicy = retentionPolicy == null || retentionPolicy.equals("") ? "autogen" : retentionPolicy;
      //  copyDB();
    }



    /**
     * Create database
     *
     * @param dbName

    // @SuppressWarnings("deprecation")
    public void createDB(String dbName) throws IOException {
        influxDB.createDatabase(dbName);
        System.out.println("Influxdb connection successful");
    }
     */
}