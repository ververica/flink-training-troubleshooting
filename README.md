# Flink Training - Troubleshooting Flink

## Introduction

## Infrastructure

There are two ways to run this job: locally in your IDE and on dA Platform. 

### Locally

Just run the main-method of `TroubledStreamingJob`. To have access to the Flink UI, replace the existing `ExecutionEnvironment` by `StreamExecutionEnvironment.createLocalEnvironmentWithWebUi(new Configuration())`. You might also want use `DataStream#print()` instead of the `DiscardingSink` for local debugging.

### dA Platform*

Each participant has access to a personal dA Platform instance (running on a single-node Kubernetes cluster) for the duration of the training. You should have received your IP address `DAP_ADDRESS` offline.

* Application Manager is accessible via `DAP_ADDRESS`:30000. 
* Minio (an S3-compatible FS) is accessible via `DAP_ADDRESS`:30001

`mvn install` will be automatically upload your artifact and change your dA Platform Deployment specification to point to the new artifact. For this you need to change the line `<daplatform.ip>CHANGE_ME</daplatform.ip>` in `pom.xml` to point to your personal dA Platform instance's IP addresss. The dA Platform UI can be accessed via your dA Platform instances IP on port 3000. Minio (an S3 mock we use for artifact storage as well as savepoints/checkpoints during the training) can be accessed on port 30001. 

Application Manager already contains a deployment for this `TroubledStreamingJob`, which you can start via the UI once you have uploaded the artifact to Minio (see above). Once the job is started, Application Manager gives you access to: 

* Flink UI
* Logs (Kibana)
* Metrics (Grafana; username: admin, password: admin)

## The Flink Job


This simple Flink job reads measurement data from a Kafka topic with eight partitions. For the purpose of this training the `KafkaConsumer` is replaced by `FakeKafkaSource`. These measurements are enriched with the current temperature based on the `location` of the measurement. Afterwards, the temperature is averaged over 30 seconds. The overall flow is depicted below:

Source -> Watermarks/Timestamps -> Deserialization -> Enrichment -> Filter/Projection -> Windowed Aggregation -> Sink  

The Enrichment uses a `TemperatureClient`, which  - for the purpose of this training - simulates requests to an API over the network. 

# Exercises

## Getting Started

1. Run `TroubleStreamingJob` locally and check out the Flink UI.
2. Run `mvn install` and start the deployment via dA Application Manager on `DAP_ADDRESS`:30000. Once it has reached the "RUNNING" state, find and check out out the Flink UI, metrics dashboard and logs.

## Correctness & Robustness Issues

1. In "Getting Started" you probably have noticed that your job is frequently restarting in the IDE as well as on dA Platform. Fix this issue and redeploy to dA Platform.

2. Now the job is running stable, but there is no output. Investigate the issue and fix it. The Flink UI might help with this.

## Performance Issues

1. Identify which task or operator currently is the bottleneck by using the backpressure monitor of the Flink UI. How could this operator be improved?

More to come...
