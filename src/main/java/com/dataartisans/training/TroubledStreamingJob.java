package com.dataartisans.training;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.dataartisans.training.entities.WindowedMeasurements;
import com.dataartisans.training.source.SourceUtils;
import com.dataartisans.training.udfs.EnrichMeasurementWithTemperature;
import com.dataartisans.training.udfs.MeasurementAggregationFunction;
import com.dataartisans.training.udfs.MeasurementDeserializer;
import com.dataartisans.training.udfs.MeasurementTSExtractor;
import com.dataartisans.training.udfs.MeasurementWindowFunction;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.concurrent.TimeUnit;

public class TroubledStreamingJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //Time Characteristics
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(500);

        //Checkpointing Configuration
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4000);

        //Restart Strategy (always restart)
        env.setRestartStrategy(RestartStrategies.failureRateRestart(10,
                org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS),
                org.apache.flink.api.common.time.Time.of(1, TimeUnit.SECONDS)));


        DataStream<JsonNode> sourceStream = env
                .addSource(SourceUtils.createFakeKafkaSource()).name("FakeKafkaSource")
                .assignTimestampsAndWatermarks(new MeasurementTSExtractor())
                .map(new MeasurementDeserializer()).name("Deserialization");

        DataStream<JsonNode> enrichedStream = sourceStream
                .keyBy(jsonNode -> jsonNode.get("location").asText())
                .map(new EnrichMeasurementWithTemperature(10000)).name("Enrichment");

        SingleOutputStreamOperator<WindowedMeasurements> avgValuePerLocation = enrichedStream
                .keyBy(jsonNode -> jsonNode.get("location").asText())
                .timeWindow(Time.of(1, TimeUnit.SECONDS))
                .aggregate(new MeasurementAggregationFunction(), new MeasurementWindowFunction())
                .name("WindowedAggregationPerLocation");

        avgValuePerLocation.addSink(new DiscardingSink<>()); //use for performance testing in dA Platform
//        avgValuePerLocation.print(); //use for local testing

        env.execute();
    }

}
