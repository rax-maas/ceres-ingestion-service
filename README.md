# Ingestion Service
Ingestion Service goal is to move data from `Kafka` to `InfluxDB`. This service moves `raw` OR `rolledup` metrics data based on spring profile in use, based on profiles.

## Setup
Install docker. Once done with that, you can use [`test-infrastructure`](https://github.com/racker/ceres-test-infrastructure) repository to install and run `Kafka`, `InfluxDB` and `Redis`. Please follow instruction from that repository to install them. Ingestion Service needs `Kafka` and `InfluxDB` only. Though ingestion-service depends on tenant-routing-service, we don't need to run the service for development setup. Use `development` spring profile for development work. When you are using this profile, tenant-routing-service is stubbed to return specific routing information.

To run or test ingestion-service locally:

- Get repo [`ceres-test-data-generator`](https://github.com/racker/ceres-test-data-generator)
- Go to `ceres-test-data-generator` folder locally
- Build repo project, i.e. `mvn clean install`  
- Run `java -jar target/test-data-generator-0.0.1-SNAPSHOT.jar` This will create raw test data into Kafka.
  
## Spring Boot Profiles
As defined in `src/main/resources/application.properties`

### `raw-data-consumer`
Moves raw metrics data.

### `rollup-data-consumer`
Moves rolled-up data.

### `development`
The profile to use in development.
  
## Running using IntelliJ
- make sure containers from `test-infrastructure` are running. Make sure you ran this -> `docker-compose up -d`
- make sure in `test-data-generator` you are running following command on the terminal:
  `java -jar target/test-data-generator-0.0.1-SNAPSHOT.jar`
  
Now, you can run `ingestion-service` using IntelliJ
