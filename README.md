
**⚠️ This repository was archived and it's content was moved to https://github.com/ververica/flink-training/ ⚠️**

---

# Apache Flink® Troubleshooting Training

## Introduction

This repository provides the basis of the hands-on part of the "Apache Flink Troubleshooting" training session at Flink Forward Europe 2019.  

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

During the training, participants will be asked to run the Flink job `TroubledStreamingJob` locally as well as on Ververica Platform.

### Running Locally

Just run the test in `TroubledStreamingJobRunner` which will call the main-method of `TroubledStreamingJob` with a local configuration and automatically pulls in dependencies with "provided" scope.

Once running, you can access Flink's Web UI via http://localhost:8081.

### The Flink Job

This simple Flink job reads measurement data from a Kafka topic with eight partitions. For the purpose of this training the `KafkaConsumer` is replaced by `FakeKafkaSource`. The result of a calculation based on the measurement value is averaged over 1 second. The overall flow is depicted below:

```
+-------------------+     +-----------------------+     +-----------------+     +----------------------+     +--------------------+
|                   |     |                       |     |                 |     |                      |     |                    |
| Fake Kafka Source | --> | Watermarks/Timestamps | --> | Deserialization | --> | Windowed Aggregation | --> | Sink: NormalOutput |
|                   |     |                       |     |                 |     |                      |     |                    |
+-------------------+     +-----------------------+     +-----------------+     +----------------------+     +--------------------+
                                                                                            \
                                                                                             \               +--------------------+
                                                                                              \              |                    |
                                                                                               +-----------> | Sink: LateDataSink |    
                                                                                                             |                    |
                                                                                                             +--------------------+
```

In local mode, sinks print their values on `stdout` (NormalOutput) and `stderr` (LateDataSink) for simplified debugging while as without local mode, a `DiscardingSink` is used for each sink.

----

*Apache Flink, Flink®, Apache®, the squirrel logo, and the Apache feather logo are either registered trademarks or trademarks of The Apache Software Foundation.*
