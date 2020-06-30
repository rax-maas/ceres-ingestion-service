package com.rackspacecloud.metrics.ingestionservice.influxdb.providers;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import java.util.List;
import  com.rackspacecloud.metrics.ingestionservice.influxdb.providers.CreateInfluxDB;
import  com.rackspacecloud.metrics.ingestionservice.influxdb.providers.NotcreatedDBInfluxDB;

//import org.springframework.kafka.test.context.EmbeddedKafka;
public class RestoreDBInfluxDB
{

    public static void main(String[] args) {

        CreateInfluxDB influxDBConnection1 = new CreateInfluxDB("","","","","");
        NotcreatedDBInfluxDB ndb = new NotcreatedDBInfluxDB();
        ndb.influxDbBuild();
    }

}

