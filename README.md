# Ingestion Service
Ingestion Service goal is to move data from `Kafka` to `InfluxDB`. This service moves `raw` OR `rolledup` metrics data based on spring profile in use, based on profiles.

## Setup
Install docker. Once done with that, you can use [`test-infrastructure`](https://github.com/racker/ceres-test-infrastructure) repository to install and run `Kafka`, `InfluxDB` and `Redis`. Please follow instruction from that repository to install them. Ingestion Service needs `Kafka` and `InfluxDB` only. Though ingestion-service depends on tenant-routing-service, we don't need to run the service for development setup. Use `development` spring profile for development work. When you are using this profile, tenant-routing-service is stubbed to return specific routing information.

To run or test ingestion-service locally:

- Get repo `ingestion-service-functional-test`
- Go to `ingestion-service-functional-test` folder locally
- Build repo project, i.e. `mvn clean install`  
- Run `java -jar target/ingestion-service-functional-test-0.0.1-SNAPSHOT.jar` This will create raw test data into Kafka.
  
## Spring Boot Profiles
As defined in `src/main/resources/application.properties`

### `raw-data-consumer`
Moves raw metrics data.

### `rollup-data-consumer`
Moves rolled-up data.

### `development`
The profile to use in development.
  
## Running using IntelliJ
Setup the following environment variables:

  - Environment variable `TEST_KAFKA_BOOTSTRAP_SERVERS` with value `localhost:9092`
  - Environment variable `LISTENER_CONTAINER_IDLE_INTERVAL` with value `30000` (?)
  - Environment variable `SESSION_TIMEOUT_MS_CONFIG` with value `30000` (?)
