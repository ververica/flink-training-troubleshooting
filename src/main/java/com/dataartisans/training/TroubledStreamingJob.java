package com.dataartisans.training;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import com.dataartisans.training.entities.WindowedMeasurements;
import com.dataartisans.training.source.SourceUtils;
import com.dataartisans.training.udfs.EnrichMeasurementWithTemperature;
import com.dataartisans.training.udfs.MeasurementDeserializer;
import com.dataartisans.training.udfs.MeasurementTSExtractor;
import com.dataartisans.training.udfs.MeasurementWindowAggregatingFunction;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class TroubledStreamingJob {

    public static void main(String[] args) throws Exception {
        final boolean throttled = Arrays.asList(args).contains("throttled");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //Time Characteristics
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(2000);

        //Checkpointing Configuration
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4000);

        //Restart Strategy (always restart)
        env.setRestartStrategy(RestartStrategies.failureRateRestart(10,
                org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS),
                org.apache.flink.api.common.time.Time.of(1, TimeUnit.SECONDS)));

        DataStream<JsonNode> sourceStream = env
                .addSource(SourceUtils.createFakeKafkaSource(throttled)).name("FakeKafkaSource")
                .assignTimestampsAndWatermarks(new MeasurementTSExtractor())
                .map(new MeasurementDeserializer()).name("Deserialization");

        DataStream<JsonNode> enrichedStream = sourceStream
                .keyBy(jsonNode -> jsonNode.get("location").asText())
                .map(new EnrichMeasurementWithTemperature(10000)).name("Enrichment");

        OutputTag<JsonNode> lateDataTag = new OutputTag<JsonNode>("late-data") {
            private static final long serialVersionUID = 33513631677208956L;
        };

        SingleOutputStreamOperator<WindowedMeasurements> aggregatedPerLocation = enrichedStream
                .keyBy(jsonNode -> jsonNode.get("location").asText())
                .timeWindow(Time.of(1, TimeUnit.SECONDS))
                .sideOutputLateData(lateDataTag)
                .process(new MeasurementWindowAggregatingFunction())
                .name("WindowedAggregationPerLocation");

        aggregatedPerLocation.addSink(new DiscardingSink<>()).name("NormalOutput").disableChaining(); //use for performance testing in dA Platform
//        aggregatedPerLocation.print().name("output"); //use for local testing

        aggregatedPerLocation.getSideOutput(lateDataTag).addSink(new DiscardingSink<>()).name("LateDataSink").disableChaining();

        env.execute();
    }

}
