# Apache Flink® Troubleshooting Training

**This repository provides a training for troubleshooting Apache Flink applications.**

## Introduction

This repository provides the basis of the hands-on part of the "Troubleshooting Flink" advanced training session at Flink Forward Berlin 2018.  

### Requirements

To make use of this repository participants will need:

* git
* JDK 8
* maven
* a Java IDE (Intellij IDEA/Eclipse)
* access to a dA Platform instance (provided offline during the training) 

### Infrastructure

During the training participants will be asked to run the Flink job `TroubledStreamingJob` locally as well as on dA Platform.

### Locally

Just run the single test `TroubledStreamingJobRunner` which will call main with a local configuration and automatically pulls in dependencies with "Provided" scope. 

### dA Platform

Each participant has access to a personal dA Platform instance (running on a single-node Kubernetes cluster) for the duration of the training. You should have received your IP address `DAP_ADDRESS` offline.

* Application Manager is accessible via `DAP_ADDRESS`:30000. 
* Minio (an S3-compatible FS) is accessible via `DAP_ADDRESS`:30001 (username: admin, password: password)

`mvn install` will automatically upload your artifact to `DAP_ADDRESS`:30001/flink/flink-training-troubleshooting-0.1.jar. For this you need to change the line `<daplatform.ip>CHANGE_ME</daplatform.ip>` in `pom.xml` to point to your personal dA Platform instance's IP addresss. The dA Platform UI can be accessed via your dA Platform instances IP on port 3000. Minio - an S3 mock we use for artifact storage as well as savepoints/checkpoints during the training - can be accessed on port 30001. If the automatic upload fails for some reasons, just run `mvn package` and upload the `target/flink-training-troubleshooting-0.1.jar` via the Minio UI (`DAP_ADDRESS`:30001).

Application Manager already contains a deployment for `TroubledStreamingJob`, which you can start via the UI once you have uploaded the artifact to Minio (see above). Once the job is started, Application Manager gives you access to: 

* Flink UI
* Logs (Kibana)
* Metrics (Grafana; username: admin, password: admin)

The pre-built job-specific dashboard consists of four rows:

1. General health statistics for the job (uptime, number of restarts, completed/failed checkpoints).
2. Number of records emitted per second from each subtask of the Kafka source. Increasing this value is our **main objective** for the performance debugging part of the training (see below). Additionally, the number of records served from the temperature cache per second.
3. Details about the output of the job: output per second, late records per second as well as the latency distribution of the records after aggregation.
4. More details about the duration and alignment phase of the last checkpoints.


### The Flink Job

This simple Flink job reads measurement data from a Kafka topic with eight partitions. For the purpose of this training the `KafkaConsumer` is replaced by `FakeKafkaSource`. These measurements are enriched with the current temperature based on the `location` of the measurement. Afterwards, the result of a calculation based on temperature and measurement value is averaged over 1 second. The overall flow is depicted below:

Source -> Watermarks/Timestamps -> Deserialization -> Enrichment -> Windowed Aggregation -> Sink  

The enrichment uses a `TemperatureClient`, which - for the purpose of this training - simulates requests to an API over the network. The enrichment operator caches the temperature for each location. The timeout of this cache is given as the parameter `cacheExpiryMs` to the constructor. The cache timeout should be seen as a business requirement and is not a parameter, which can be change for pure performance tuning purposes.

## Exercises

General Note: Classes, methods or fields marked annotated with `@DoNotChangeThis` should not be changed by training participants. They are either part of the required business logic, or usually outside of the scope of the Flink in a real-life scenario.

### Getting Started

1. Run `TroubledStreamingJob` locally and check out the Flink UI.
2. Run `mvn install` and start the deployment via dA Application Manager on `DAP_ADDRESS`:30000. Once it has reached the "RUNNING" state, find and check out out the Flink UI, metrics dashboard and logs. In particular, you should familarize yourself with the metrics.

### Correctness & Robustness Issues

1. In "Getting Started", you have probably noticed that your job is frequently restarting in the IDE as well as on dA Platform. Fix this issue and redeploy to dA Platform.

Note: If you upload a new version of the jar via `mvn install` you need to "Cancel" or "Suspend" the Deployment and then "Start" it again for the changes to take effect.

2. Now the job is running stable, but there is no output. Investigate the issue and fix it. The Flink UI might help with this.

### Performance Issues

1. Identify which task or operator currently is the bottleneck by using the backpressure monitor of the Flink UI. How could this operator be improved?

For the following exercises it is important to keep the `cacheExpiryMs` parameter stable. You can just leave the parameter at the default value from the previous exercise. When profiling your Flink job with VisualVM or similar the bottlenecks will be most visible, if you set `cacheExpiryMs` to `Integer.MAX_VALUE`, which basically circumvents any "external IO" after a short warm-up phase.

2. Improve the throughput of `TroubledStreamingJob` by making serialization more efficient. You can expect an improvement of a factor of 1.5 - 2. Hints for inefficient serializations can be found in the logs of the Flink job.

3. Improve the throughput of `TroubledStreamingJob` further by identifying any inefficient user code. The backpressure monitor as well as a profiler like VisualVM can help you to identify suspicious operators. Improvements of a factor of more than 2 should still be possible.

### Latency Issues (Bonus)

For this bonus task, we will look at the event time lag of the `WindowedAggregationPerLocation` operator, i.e. the time that the reporting of a window result lags behind processing time. We will run the job from above with a slightly altered behaviour that throttles the source and has some more heavy computation in the window operator.

 *Please note that some lag is inherent in our source definitions (some sources' timestamps lag behind others) as well as our application's business logic (max out of orderness in the watermark generation). These should not be changed!*

1. Run the Flink job with the `--latencyUseCase` argument by adding it to
   - the deployment configuration (in dA Application Manager)
   - the program arguments (locally in IntelliJ IDEA)
2. Reduce the 99th percentile of the event time lag of the `WindowedAggregationPerLocation` operator. The `eventTimeLag` metric will show the current value.

----

*Apache Flink, Flink®, Apache®, the squirrel logo, and the Apache feather logo are either registered trademarks or trademarks of The Apache Software Foundation.*
