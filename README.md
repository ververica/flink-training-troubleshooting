# Flink Training - Troubleshooting Flink

## Introduction

## Infrastructure

There are two ways to run this job: locally in your IDE and on dA Platform. 

### Locally

Just run the main-method of `TroubledStreamingJob`. To have access to the Flink UI, replace the existing `ExecutionEnvironment` by `StreamExecutionEnvironment.createLocalEnvironmentWithWebUi(new Configuration())`. You might also want use `DataStream#print()` instead of the `DiscardingSink` for local debugging.

### dA Platform

Each participant has access to a personal dA Platform instance (running on a single-node Kubernetes cluster) for the duration of the training. You should have received your IP address `DAP_ADDRESS` offline.

* Application Manager is accessible via `DAP_ADDRESS`:30000. 
* Minio (an S3-compatible FS) is accessible via `DAP_ADDRESS`:30001 (username: admin, password: password)

`mvn install` will automatically upload your artifact to `DAP_ADDRESS`:30001/flink/flink-training-troubleshooting-0.1.jar. For this you need to change the line `<daplatform.ip>CHANGE_ME</daplatform.ip>` in `pom.xml` to point to your personal dA Platform instance's IP addresss. The dA Platform UI can be accessed via your dA Platform instances IP on port 3000. Minio (an S3 mock we use for artifact storage as well as savepoints/checkpoints during the training) can be accessed on port 30001. If the automatic upload fails for some reasons, just run `mvn package` and upload the `target/flink-training-troubleshooting-0.1.jar` via the Minio UI (`DAP_ADDRESS`:30001).

Application Manager already contains a deployment for `TroubledStreamingJob`, which you can start via the UI once you have uploaded the artifact to Minio (see above). Once the job is started, Application Manager gives you access to: 

* Flink UI
* Logs (Kibana)
* Metrics (Grafana; username: admin, password: admin)

The pre-built job-specific dashboard consists of four rows. 

1. General health statistics for the job (uptime, number of restarts, completed/failed checkpoints). 
2. Number of records emitted per second from each subtask of the Kafka source. Increasing this value is our **main objective** for the performance debugging part of the training (see below).
3. More details on the throughput of the job at different stages. 
4. More details about the duration and alignment phase of the last checkpoints.


## The Flink Job

This simple Flink job reads measurement data from a Kafka topic with eight partitions. For the purpose of this training the `KafkaConsumer` is replaced by `FakeKafkaSource`. These measurements are enriched with the current temperature based on the `location` of the measurement. Afterwards, the temperature is averaged over 30 seconds. The overall flow is depicted below:

Source -> Watermarks/Timestamps -> Deserialization -> Enrichment -> Filter/Projection -> Windowed Aggregation -> Sink  

The enrichment uses a `TemperatureClient`, which - for the purpose of this training - simulates requests to an API over the network. The enrichment operator caches the temperature for each location. The timeout of this cache is given as the parameter `cacheExpiryMs` to the constructor. The cache timeout should be seen as a business requirement and is not a parameter, which we can change for pure performance tuning purposes.

# Exercises

## Getting Started

1. Run `TroubledStreamingJob` locally and check out the Flink UI.
2. Run `mvn install` and start the deployment via dA Application Manager on `DAP_ADDRESS`:30000. Once it has reached the "RUNNING" state, find and check out out the Flink UI, metrics dashboard and logs. In particular, you should familarize yourself with the metrics.

## Correctness & Robustness Issues

1. In "Getting Started" you probably have noticed that your job is frequently restarting in the IDE as well as on dA Platform. Fix this issue and redeploy to dA Platform.

Remark: If you upload a new version of the jar via `mvn install` you need to "Cancel" or "Suspend" the Deployment and then "Start" it again for the changes to take effect.

2. Now the job is running stable, but there is no output. Investigate the issue and fix it. The Flink UI might help with this.

## Performance Issues

1. Identify which task or operator currently is the bottleneck by using the backpressure monitor of the Flink UI. How could this operator be improved?

For the following exercises it is important to keep the `cacheExpiryMs` parameter stable. You can just leave the parameter at the default value from the previous exercise. When profiling your Flink job with VisualVM or similar the bottlenecks will be most visible, if you set `cacheExpiryMs` to `Integer.MAX_VALUE`, which basically circumvents any "external IO" after a short warm-up phase.

2. Improve the throughput of `TroubledStreamingJob` by making serialization more efficient. You can expect an improvement of a factor of 1.5 - 2. Hint for inefficient serialization can be found in the logs of the Flink Job.

3. Improve the throughput of `TroubledStreamingJob` further by identifying and inefficient user code. The backpressure monitor as well as a profiler like VisualVM can help you to identify suspicious operators.