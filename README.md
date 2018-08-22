# Flink Training - Troubleshooting Flink

## Introduction

## Infrastructure

There are two ways to run this job: locally in your IDE and on dA Platform. 

* **Locally:** Just run the main-method of `TroubledStreamingJob`. To have access to the Flink UI, replace the existing `ExecutionEnvironment` by `StreamExecutionEnvironment.createLocalEnvironmentWithWebUi(new Configuration())`

* **dA Platform** Each participant has access to a personal dA Platform instance (running on a single-node Kubernetes cluster) for the duration of the training. `mvn install` will be automatically upload your artifact and change your dA Platform Deployment specification to point to the new artifact. For this you need to change the line `<daplatform.ip>CHANGE_ME</daplatform.ip>` in `pom.xml` to point to your personal dA Platform instance's ip addresss. The dA Platform UI can be accessed via your dA Platform instances IP on port 3000. Minio (an S3 mock we use for artifact storage as well as savepoints/checkpoints during the training) can be accessed on port 30001. 

## The Flink Job

TODO

## Exercises

### Correctness

1. Run `mvn install` and check under the dA Platform UI for the corresponding deployment. You can also access the Flink UI from the Deployment overview in dA Platform, once the job is running. 
You will notice that your job is frequently restarting. The same happens when you run the job locally in your IDE. Fix this issue and redeploy to dA Platform.

2. Now the job is running stable, but there is no output. Investigate the issue and fix it. The FlinkUI might help with this. (Did you now you can start the Flink UI locall with `))

### Performance

1. Identify which task or operator currently is the bottleneck by using the the backpressure monitor of the Flink UI. How could this operator be improved?

More to come...
