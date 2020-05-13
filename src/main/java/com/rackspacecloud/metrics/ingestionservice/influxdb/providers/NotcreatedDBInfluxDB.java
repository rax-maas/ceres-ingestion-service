package com.rackspacecloud.metrics.ingestionservice.influxdb.providers;


import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.NumberUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class NotcreatedDBInfluxDB {

    private InfluxDB influxDB;

    float bkuprowcountval;
    float resrowcountval;
    List<Float> al=new ArrayList<Float>();
    public List<Object> bk=new ArrayList<Object>();
    List<Object> bkrwcnt=new ArrayList<Object>();
    List<Object> resrwcnt=new ArrayList<Object>();

    public  InfluxDB influxDbBuild()
    {
        InfluxDB influxDB = null;
        OkHttpClient.Builder client = new OkHttpClient.Builder()
                .connectTimeout(1, TimeUnit.MINUTES)
                .readTimeout(1, TimeUnit.MINUTES)
                .writeTimeout(1, TimeUnit.MINUTES)
                .retryOnConnectionFailure(true);

        if (influxDB == null) {
            influxDB = InfluxDBFactory.connect("http://localhost:8086","*****","*****", client);
        }



        //influxDB = InfluxDBFactory.connect(openurl, username, password, client);

        try {
            if (!influxDB.databaseExists("db_1")) {
                //  influxDB.createDatabase(database);
                String createRetPolicyString =
                        String.format("CREATE RETENTION POLICY \"non_existing_rp\" " +
                                "ON \"%s\" DURATION 5d REPLICATION 1 DEFAULT", "db_1");
                // Get Column values
                // Query query = new Query("SHOW MEASUREMENTS", database);
                // }else {
                //   System.out.println("DB already exists");
            }
        } catch (Exception e) {
            // The database may set up dynamic proxy, and does not support creating database.
            e.printStackTrace();
        }
        // Read list of DBs in the InfluxDb
        Query dbquery = new Query("SHOW DATABASES", null);
        QueryResult queryResult = influxDB.query(dbquery);
        List<QueryResult.Result> dbresults = queryResult.getResults();
        List<List<Object>> finaldbresults = dbresults.get(0).getSeries().get(0).getValues();
        int bkdbcount = 0;
        int bktblecount = 0;
        for (int i = 0; i < finaldbresults.size(); i++) {
            List<Object> dbnames = finaldbresults.get(i);
            for (Object dbname : dbnames) {
              //Query query = new Query("SHOW MEASUREMENTS", dbname.toString());
                Query query = new Query("SHOW MEASUREMENTS", "db_0");
                    List<QueryResult.Result> results = influxDB.query(query).getResults();
                    List<List<Object>> rawMetricNames = results.get(0).getSeries().get(0).getValues();

                bktblecount = 0;
                for (Object bktable : rawMetricNames) {
                    // System.out.println("Total number of databases in DB_0  ----" + rawMetricNames);
                    if (rawMetricNames.isEmpty() || rawMetricNames == null) {
                        System.out.println("Tables not available for ----" + dbname);
                    } else {
                       // System.out.println("Tables available in  ----" + rawMetricNames);
                    }
                    bktblecount = bktblecount+1;
                    //  }
                }
            }
            bkdbcount = i + 1;
        }
        //System.out.println("Total DB count in backup db---" + bkdbcount);
        //System.out.println("Total table count in backup db---" + bktblecount);

        influxDB.createDatabase("db_1");
      //  System.out.println("Restore database has been created");

        // Copy all measurements from db_0 to Db_1 restore database.
        QueryResult copyDb = influxDB.query(new Query("SELECT * INTO db_1.autogen.:MEASUREMENT FROM db_0.rp_5d./.*/ GROUP BY *", "db_0"));
       // System.out.println("Copy of all databases, measurements and datas has been restored");

        // Compare Backup and the Restore DB
        try {

            // Read list of databases in restore db
            Query resdbquery = new Query("SHOW DATABASES", "db_1");
            QueryResult resqueryResult = influxDB.query(resdbquery);
            List<QueryResult.Result> resdbresults = resqueryResult.getResults();
            List<List<Object>> resfinaldbresults = resdbresults.get(0).getSeries().get(0).getValues();
          //  System.out.println("Databases available in the restore db ---" + resfinaldbresults);

            long restoretblecount = 0;
            int resdbcount = 0;
            for (int i = 0; i < resfinaldbresults.size(); i++) {
                List<Object> dbnames = resfinaldbresults.get(i);
                resdbcount = resdbcount+1;
            }

          //  System.out.println("Total DB count in restore db---" + resdbcount);

            // SHow measurements in Backup DB db_0

            Query bkquery = new Query("SHOW MEASUREMENTS", "db_0");
            List<QueryResult.Result> bkdbbresults = influxDB.query(bkquery).getResults();
            List<List<Object>> bkdbtble = bkdbbresults.get(0).getSeries().get(0).getValues();
            //System.out.println("Tables in Restore Db db_0  ----" + bkdbtble);

            for (int i = 0; i < bkdbtble.size(); i++) {
                List<Object> backuptble = bkdbtble.get(i);
                for (Object bktable : backuptble) {
                    QueryResult bkrowcount = influxDB.query(new Query("SELECT count(*) FROM " + bktable, "db_0"));
                    List<QueryResult.Result> bkrowcountResults = bkrowcount.getResults();
                   // System.out.println("rowcounts of table in backup db  db_0  ----" + bkrowcountResults);

                    if(bkrowcountResults.get(0).hasError()) {
                    }else{
                        List<List<Object>> backupdbtblecnt = bkrowcountResults.get(0).getSeries().get(0).getValues();
                       // bk.add(bkrowcountResults.get(0).getSeries().get(0).getValues());

                      //   System.out.println("RowCount in backup db table  "  + backupdbtblecnt);

                        // Get the row count of each table from the restore database

                      for (int j = 0; j < backupdbtblecnt.size(); j++) {
                            List<Object> backuprowcount = backupdbtblecnt.get(j);
                             bkuprowcountval = Float.valueOf(backuprowcount.get(1).toString());
                             bkrwcnt.add(Float.valueOf(backuprowcount.get(1).toString()));
//                          System.out.println("Row count from each table in backup db " + bkuprowcountval);

                        }

                    }

                }
            }

            // Show measurements in restore Db db_1

            int restblecount = 0;
            Query query = new Query("SHOW MEASUREMENTS", "db_1");
            List<QueryResult.Result> restoredbresults = influxDB.query(query).getResults();
            List<List<Object>> restoredbtble = restoredbresults.get(0).getSeries().get(0).getValues();
//            System.out.println("Tables in Restore Db db_1  ----" + restoredbtble);

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
                    restblecount = i+1;
                }
            }

//            System.out.println("Total Table in restore db  "  + restblecount);

            // Compare total db count in backup db and restore db

            if(bkdbcount == resdbcount) {
//                System.out.println("DB count equal in Backup and Restore DB");
            }else
            {
//                System.out.println("DB count not equal in Backup and Restore DB");
            }

            // Compare total table count in backup db and restore db

            if(bktblecount == restblecount) {
             //   System.out.println("Table count equal in Backup and Restore DB");
            }else
            {
               // System.out.println("Table count not equal in Backup and Restore DB");
            }

            // Compare total rowcount in backup db and restore db
            if(bkrwcnt.equals(resrwcnt)){
         //       System.out.println("Rowcount in all the tables are equal!!!!");
            }
            else{
           //     System.out.println("Rowcount are not correct!!!!");
            }

        }
        catch (Exception e){
            if(e.equals(null)) {
                System.out.println("False");
            }
            e.printStackTrace();
        }

        return influxDB;

    }



}