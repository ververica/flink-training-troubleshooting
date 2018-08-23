package com.dataartisans.training;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TroubledStreamingJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new FakeKafkaSource()).print();

        env.execute();
    }

}
