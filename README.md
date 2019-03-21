# Apache Flink® Troubleshooting Training

## Introduction

This repository provides the basis of the hands-on part of the "Apache Flink Troubleshooting & Operations" training session at Flink Forward San Franciscos 2019.  

### Requirements

To make use of this repository participants will need:

* git
* JDK 8
* maven
* a Java IDE (Intellij IDEA/Eclipse)

### Training Preparations

In order to avoid potential issues with the WiFi at the training venue, please checkout and build the project prior to the training:

```bash
git clone git@github.com:ververica/flink-training-troubleshooting.git
cd flink-training-troubleshooting
mvn clean package
```

### Infrastructure

During the training participants will be asked to run the Flink job `TroubledStreamingJob` locally as well as on Ververica Platform.

### Running Locally

Just run the single test `TroubledStreamingJobRunner` which will call the main-method of TroubledStreamingJob with a local configuration and automatically pull in dependencies with "Provided" scope.

### The Flink Job

This simple Flink job reads measurement data from a Kafka topic with eight partitions. For the purpose of this training the `KafkaConsumer` is replaced by `FakeKafkaSource`. The result of a calculation based on the measurement value is averaged over 1 second. The overall flow is depicted below:

```
+---------------------+       +----------------------+       +----------------------+       +-----------------------+       +----------------------+
|                     |       |                      |       |                      |       |                       |       |                      |
|  Fake Kafka Source  | +---> | Watermarks/Timstamps | +---> |    Deserialization   | +---> | Windowed Aggreagation | +---> |    Discaring Sink    |
|                     |       |                      |       |                      |       |                       |       |                      |
+---------------------+       +----------------------+       +----------------------+       +-----------------------+       +----------------------+
```
----

*Apache Flink, Flink®, Apache®, the squirrel logo, and the Apache feather logo are either registered trademarks or trademarks of The Apache Software Foundation.*
