package com.rackspacecloud.metrics.ingestionservice;

import com.rackspacecloud.metrics.ingestionservice.influxdb.config.BackupProperties;
import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.CreateInfluxDB;
import com.rackspacecloud.metrics.ingestionservice.influxdb.providers.NotcreatedDBInfluxDB;
import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles(value = { "test" })
@EmbeddedKafka(partitions = 1, topics = { IngestionServiceApplicationTests.UNIFIED_METRICS_TOPIC })
@EnableConfigurationProperties(BackupProperties.class)

public class CreateInfluxDBTests {

    private String username = "Ravi5626";
    private String password = "*******";
    private String openurl = "http://localhost:8086";
    private String database = "db_1";
    private String retentionPolicy = "autogen";

    @Mock
    private InfluxDB influxDB;
    private NotcreatedDBInfluxDB notcreatedDBInfluxDB = new NotcreatedDBInfluxDB();

    @Before
    public void setUp() {
        //    InfluxDB influxDB = null;
        OkHttpClient.Builder client = new OkHttpClient.Builder()
                .connectTimeout(1, TimeUnit.MINUTES)
                .readTimeout(1, TimeUnit.MINUTES)
                .writeTimeout(1, TimeUnit.MINUTES)
                .retryOnConnectionFailure(true);

        if (influxDB == null) {
            //influxDB = InfluxDBFactory.connect("http://localhost:8086","Ravi5626","Change@555", client);
            influxDB = InfluxDBFactory.connect(openurl,username,password, client);
        }
    }

    @Test
    public void isDBExistinRestore(){
        //     CreateInfluxDB influxDBConnection1 = new CreateInfluxDB("","","","","");
        CreateInfluxDB influxDBConnection1 = new CreateInfluxDB(username,password,openurl,database,retentionPolicy);
        Query dbquery = new Query("SHOW DATABASES", null);
        List<QueryResult.Result> dbresults = new ArrayList<>();
        QueryResult queryRes= new QueryResult();
        List resultList= new ArrayList();
        resultList.add(("db_1"));
        queryRes.setResults(resultList);
        QueryResult result = new QueryResult();
        result.setResults(resultList);
        Mockito.when(influxDB.query(Mockito.any())).thenReturn(result);
        assertTrue(result.getResults().size() == 1);
        System.out.println("Restore database created and available=====" + resultList);
        System.out.println("Restore database has been created");
    }
    @Test
    public void isNotDBExistinRestore(){

//        CreateInfluxDB influxDBConnection1 = new CreateInfluxDB("","","","","");
        CreateInfluxDB influxDBConnection1 = new CreateInfluxDB(username,password,openurl,database,retentionPolicy);
        Query dbquery = new Query("SHOW DATABASES", null);
        List<QueryResult.Result> dbresults = new ArrayList<>();
        QueryResult queryRes= new QueryResult();
        List resultList= new ArrayList();
        queryRes.setResults(resultList);
        QueryResult result = new QueryResult();
        result.setResults(resultList);
        Mockito.when(influxDB.query(Mockito.any())).thenReturn(result);
        assertTrue(result.getResults().size() == 0);
        System.out.println("Restore database not available=====" + resultList);

    }

    @Test
    public void BackupDBCount(){
        InfluxDB influxDB = notcreatedDBInfluxDB.influxDbBuild();
        Query dbquery = new Query("SHOW DATABASES", null);
        QueryResult queryResult = influxDB.query(dbquery);
        List<QueryResult.Result> bkdbresults = queryResult.getResults();
        List<List<Object>> bkfinaldbresults = bkdbresults.get(0).getSeries().get(0).getValues();
        assertTrue(bkfinaldbresults.size() == 5);
        System.out.println("Backup database Size=====" + bkfinaldbresults.size());
        System.out.println("Total DB count in Backup database=====" + bkfinaldbresults);
    }

    @Test
    public void RestoreDBCount(){
        InfluxDB influxDB = notcreatedDBInfluxDB.influxDbBuild();
        Query resdbquery = new Query("SHOW DATABASES", "db_1");
        QueryResult queryResult = influxDB.query(resdbquery);
        List<QueryResult.Result> resdbresults = queryResult.getResults();
        List<List<Object>> resfinaldbresults = resdbresults.get(0).getSeries().get(0).getValues();
        System.out.println("Total DB count in Restore database=====" + resfinaldbresults.size());
        assertTrue(resfinaldbresults.size() == 5);
        System.out.println("Total DB count in Restore database=====" + resfinaldbresults.size());
        System.out.println("Total DB count in Restore database=====" + resfinaldbresults);

    }

    @Test
    public void BackupTableCount() {
        InfluxDB influxDB = notcreatedDBInfluxDB.influxDbBuild();
        Query bkquery = new Query("SHOW MEASUREMENTS", "db_0");
        List<QueryResult.Result> bkdbbresults = influxDB.query(bkquery).getResults();
        List<List<Object>> bkdbtble = bkdbbresults.get(0).getSeries().get(0).getValues();
        assertTrue(bkdbtble.size() == 6);
        System.out.println("Tables in Backup Db db_0  ----" + bkdbtble);
        System.out.println("Total DB count in Backup database=====" + bkdbtble.size());
    }

    @Test
    public void RestoreTableCount(){
        InfluxDB influxDB = notcreatedDBInfluxDB.influxDbBuild();
        Query Resquery = new Query("SHOW MEASUREMENTS", "db_1");
        List<QueryResult.Result> resdbbresults = influxDB.query(Resquery).getResults();
        List<List<Object>> resdbtble = resdbbresults.get(0).getSeries().get(0).getValues();
        assertTrue(resdbtble.size() == 6);
        System.out.println("Tables in Restore Db db_1  ----" + resdbtble);
        System.out.println("Total DB count in Restore database=====" + resdbtble.size());
    }

    @Test
    public void RowCountinBackupandRestoreDB(){
        InfluxDB influxDB = notcreatedDBInfluxDB.influxDbBuild();
        List<Object> bkrwcnt=new ArrayList<Object>();
        List<Object> resrwcnt=new ArrayList<Object>();
        float bkuprowcountval;
        float resrowcountval;

        // ************************************
        Query bkquery = new Query("SHOW MEASUREMENTS", "db_0");
        List<QueryResult.Result> bkdbbresults = influxDB.query(bkquery).getResults();
        List<List<Object>> bkdbtble = bkdbbresults.get(0).getSeries().get(0).getValues();

        for (int i = 0; i < bkdbtble.size(); i++) {
            List<Object> backuptble = bkdbtble.get(i);
            for (Object bktable : backuptble) {
                QueryResult bkrowcount = influxDB.query(new Query("SELECT count(*) FROM " + bktable, "db_0"));
                List<QueryResult.Result> bkrowcountResults = bkrowcount.getResults();
                // System.out.println("rowcounts of table in backup db  db_0  ----" + bkrowcountResults);

                if(bkrowcountResults.get(0).hasError()) {
                }else{
                    List<List<Object>> backupdbtblecnt = bkrowcountResults.get(0).getSeries().get(0).getValues();
                    // Get the row count of each table from the restore database

                    for (int j = 0; j < backupdbtblecnt.size(); j++) {
                        List<Object> backuprowcount = backupdbtblecnt.get(j);
                        bkuprowcountval = Float.valueOf(backuprowcount.get(1).toString());
                        bkrwcnt.add(Float.valueOf(backuprowcount.get(1).toString()));
                    }

                }

            }
        }

        Query query = new Query("SHOW MEASUREMENTS", "db_1");
        List<QueryResult.Result> restoredbresults = influxDB.query(query).getResults();
        List<List<Object>> restoredbtble = restoredbresults.get(0).getSeries().get(0).getValues();

        for (int i = 0; i < restoredbtble.size(); i++) {
            List<Object> restoretble = restoredbtble.get(i);
            for (Object restoretable : restoretble) {
                //QueryResult rowcount = influxDB.query(new Query("SELECT count(*) FROM /.*/" , "db_1"));
                QueryResult rowcount = influxDB.query(new Query("SELECT count(*) FROM "+ restoretable  , "db_1"));
                List<QueryResult.Result> restoretbleresults =rowcount.getResults();
                if(restoretbleresults.get(0).hasError()) {
                }else{
                    List<List<Object>> resdbtblecnt = restoretbleresults.get(0).getSeries().get(0).getValues();
                    //  System.out.println("RowCount in restore db table  "  + resdbtblecnt);

                    // Get the row count of each table from the restore database

                    for (int j = 0; j < resdbtblecnt.size(); j++) {
                        List<Object> resrowcount = resdbtblecnt.get(j);
                        //   System.out.println("Row count from each table in restore db " + resrowcount.get(1));
                        resrowcountval = Float.valueOf(resrowcount.get(1).toString());
                        resrwcnt.add(Float.valueOf(resrowcount.get(1).toString()));
                        //System.out.println("Row count from each table in restore db " + resrowcountval);

                    }

                }
            }
        }
        assertTrue(bkrwcnt.equals(resrwcnt));

        if(bkrwcnt.equals(resrwcnt)){
                   System.out.println("Rowcount in all the tables are equal in restore DB!!!!");
        }
        else{
                 System.out.println("Rowcount are not correct in restore DB!!!!");
        }

        // ************************************

        System.out.println("Rowcount in all the backup table are !!!!-----" + bkrwcnt);
        System.out.println("Rowcount in all the restore table are !!!!-----"+ resrwcnt);

    }

}