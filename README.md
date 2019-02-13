# Ingestion Service
Ingestion Service goal is to move data from `Kafka` to `InfluxDB`. This service moves `raw` OR `rolledup` metrics data based on spring profile in use. If you are using `raw-data-consumer` profile, it will move `raw` metrics data, and if you are using `rollup-data-consumer` profile, it will move `rolledup` data.
## Setup
Install docker. Once done with that, you can use [`test-infrastructure`](https://github.com/racker/ceres-test-infrastructure) repository to install and run `Kafka`, `InfluxDB` and `Redis`. Please follow instruction from that repository to install them. Ingestion Service needs `Kafka` and `InfluxDB` only. Though ingestion-service depends on tenant-routing-service, we don't need to run the service for development setup. Use `development` spring profile for development work. When you are using this profile, tenant-routing-service is stubbed to return specific routing information. <br />
To run or test ingestion-service locally:
- Get repo `ingestion-service-functional-test` and after building it.
  - Go to `ingestion-service-functional-test` folder locally
  - Run `java -jar target/kafka-influxdb-functional-test-0.0.1-SNAPSHOT.jar` This will create raw test data into Kafka.
#### Running using IntelliJ
- Setup following environment variables:
  - Env variable `TEST_KAFKA_BOOTSTRAP_SERVERS` with value `localhost:9092`
  - TODO: Need to add more of env vars.


DevOps FTW!
