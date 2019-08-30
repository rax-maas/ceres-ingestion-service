## Disaster recovery

### How does it work

Disaster recovery is a process by which the ingestion service creates a copy
of all data written to InfluxDB in a format that InfluxDB would understand (line
protocol format).

In general, there are two approaches to control for database unavailability and
problems. The first approach is high availability - always keeping additional
databases upand copying the data to them as needed. This is also the approach
used by InfluxDB enterprise.

High availability is, however, quite complex. This is why, instead, a
backup-recovery approach is used to make sure problems with InfluxDB instances
are recoverable. 

### Disaster scenarios

There are multiple potential (though unlikely) disaster scenarios our production
environment could suffer from. Note that we consider all of them unlikely.

According to Google:

```
Persistent Disks have built-in redundancy to protect your data against equipment
failure and to remain available through datacenter maintenance events. Your 
instances, free of local storage, can be moved by Google Live Migration to newer
hardware without your intervention. This allows Google datacenters to be 
maintained at the highest level; software, hardware, and facilities can be 
continually updated to ensure excellent performance and reliability for your 
cloud-based services. 
```

```
Google Compute Engine uses redundant, industry-standard mechanisms to protect 
persistent disk users from data corruption and from sophisticated attacks 
against data integrity.
```

#### InfluxDB Bugs and data corruption

If InfluxDB experiences internal bugs that lead to loss of data
or data corruption. In this case, should we detect such a problem, we would be 
able to replay data for affected instances - or all instances.

#### Cluster deletion

In case our entire cluster gets deleted, including persistent volumes 
(i.e. by human mistake), we would be able to replay the data to all instances 
from backup.

#### Persistent volume deletion

As above, if a persistent volume is deleted, we would be able to replay the data
for the affected instance from backup.

#### Persistent volume corruption

A more unlikely situation, but it is possible that a container corrupts 
the database through filesystem corruption. Same as above, if detected, we can
replay the data for the instance from backup.

#### InfluxDB dropped writes

Either through a bug in InfluxDB or the ingestion service (i.e. too many series)
we may need to replay data that InfluxDB rejected. Since we don't know what 
exactly was dropped in this case, it is best to wipe and replay data for 
affected instances (likely all instances).

#### Resource unavailability

If a region becomes unavailable due to unforeseen circumstance, backup data, 
presumably stored in a multi-region backup bucket would be replayed for all
instances in a new region to bring InfluxDB instances back up.

### Recovery Process

Since we are backing up data using annotated line-protocol format, it is also
very simple to replay data into a brand new, empty instance from backup. Data
is stored in large batch files, so downloading and replaying data, even for
large deployments, should be relatively fast and easy.

#### Identify damaged instance(s)

Some of the disaster scenarios listed above make damage or data corruption to 
instances difficult to detect. At this time, we do not have a good mechanism to
detect when such problems occur. See future work and automation. 

However, more serious problems, like instance going down and data being 
unavailable are more easily spotted.

Almost all disaster cases will be handled by the same process. Once an instance
is determined as being damaged, unavailable, or corrupted, the following steps 
should be performed: 

1. Stop ingestion service

The ingestion service constantly moves data into InfluxDB instances and into
storage backup. Unless stopped, data will be partially in memory or in transit
and will be missing from the restore process. Ensure all ingestion service pods
are stopped before running recovery.

2. Wipe damaged InfluxDB Instances

The restore process is not an overwrite. Data in the instance/database/retention
policy must be wiped before it is restored or the process will result in 
duplicated measurements.

3. Download recovery files for an instance

Determine the bucket where the backup for the particular cluster is being 
stored. This setting is specified in the helm chart being used.

For example, to download all backup for instance 0 for the cluster using bucket
`ceres-backup-dev`, use

`gsutil -m cp "gs://ceres-backup-dev/data-helm-ceres-influxdb-0.influxdbsvc/**`

The `**` will ensure files in sub-directories are also downloaded.

Files are stored in the backup bucket using the following schema:
`[instance-name]/[database-name]/[retention-policy-name]/[YYYYMMDD]/[UUID].gz`

in a compressed and tagged line-protocol format. When extracted, the gz file 
should start with a snippet such as:

```
# DML
# CONTEXT-DATABASE: db_0
# CONTEXT-RETENTION-POLICY: rp_5d
``` 

This InfluxDB DML tag will ensure that data will end up in the right database 
and retention policy when importing, even when importing data from different 
databases and retention policies into the same instance.

The rest of the file is in InfluxDB text line protocol format. See 
`https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_reference/`

4. Recover using import tool

Assuming only files for the same instance recovery have been downloaded, the
instance can be recovered with a simple bash snippet such as:

```
for file in *.gz

influx -import -compressed -path=$file -precision=s

done
```  

This will simply run, in sequence, all the downloaded files using the InfluxDB's
own import tool. Since the system is designed to batch data in a somewhat large
files, the import should be comparatively fast.

5. Restart ingestion service

After importing data is finished, the ingestion service can be safely restarted.

### Testing disaster recovery

Disaster recovery using backup-restore methods is simple. A lot of the testing
is done in code (including some integration testing), and it is there to ensure
generating the backup files still works. This here serves to list some 
additional test cases and ways to verify DR is still functional.

#### Test cases

We have a set of automatic tests.

###### Unit tests

We have a set of unit tests that utilize a local-storage API that mocks the
Google store API. As such, most tests do not require actual access to Google
storage. 

###### Integration tests

Integration tests require that the environment variable 
`GOOGLE_APPLICATION_CREDENTIALS` is locally set to an appropriate json access 
key, and that the configured bucket exists and can be accessed by the service 
account specified in the variable.

If the variable is not set, the integration test will not run. Otherwise, they
will run when running maven tests, i.e. `mvn clean test`.

##### Manual testing

1. After deployment of ingestion service, wait and verify that .gz files are 
being created in the backup bucket.

2. Ensure files can be downloaded from the bucket.

3. Ensure uncompressed files contain DML at the beginning of the file and can
be imported using the InfluxDB import tool.

4. Using counting queries, ensure measurement numbers are the same. The 
expectation is that wiping and restoring an instance from backup should result 
in a close copy, though because of time passing, certain measurements may be
dropped because of retention policy.

##### Testing difficulties with permission setup

Because of how permissions should be setup in a production environment, 
currently unit/integration tests should assume that read access is not available.

Ingestion service should not have access to the backup bucket, outside of write
access. 

#### Identifying problems with InfluxDB

This needs more work. Currently there is no automatic way to detect database
problems. Implementing one may also not be easy unless complex counting that
takes retention policies into consideration is implemented.

#### Verification of recovery

Counting queries and looking for specific measurements can be used to verify
recovery was successful.

### Recovery downsides

Unlike a high-availability database, using backup-restore to implement disaster
recovery means short down-times are needed. This will be an issue with
time-sensitive problems.

### Future work

Automation and related topics, such as automatically detecting problems with
InfluxDB instances and deciding to perform an automatic restore should be 
investigated. However, this become moot in a situation where we switch to
enterprise, for example.

### Example restore script

```
#!/usr/bin/env bash

do_help () {
  cat <<EndHelp

Must be done first:
  Stop ingestion service
  Wipe damaged InfluxDB Instances
EndHelp
  exit 1
}

# comment out to run
do_help

DIR=`mktemp -d -t influxdb`
pushd $DIR

gcloud config set project ceres-dev-222017
# gsutil du -hs gs://ceres-backup-dev/influxdb-0.influxdb
gsutil -m cp 'gs://ceres-backup-dev/influxdb-0.influxdb/**' .

kubectl exec influxdb-0 -- mkdir /backup
for i in *.gz; do
  echo "uploading $i"
  kubectl cp $i influxdb-0:/backup/
done

rm -f buildtables.sql
for i in {0..9}; do
  db=db_${i}
  echo "drop database $db;" >> buildtables.sql
  echo "create database $db with duration 5d replication 1 name rp_5d;" >> buildtables.sql
done

kubectl cp buildtables.sql influxdb-0:/
kubectl exec influxdb-0 -- bash -c 'influx < /buildtables.sql'

rm -f restore.sh
cat > restore.sh <<"EndScript"
for i in /backup/*.gz; do influx -import -compressed -path=$i -precision=s; done
EndScript

kubectl cp restore.sh influxdb-0:/
kubectl exec influxdb-0 -- bash /restore.sh

popd
rm -rf $DIR
```
