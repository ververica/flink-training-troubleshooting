package com.dataartisans.training;

import com.dataartisans.training.entities.WindowedMeasurements;
import com.dataartisans.training.source.FakeKafkaSource;
import com.dataartisans.training.udfs.*;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

public class TroubledStreamingJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(500);

        env.getConfig().setLatencyTrackingInterval(100);
        env.enableCheckpointing(1000);

        //TODO Restart Strategy


        //TODO Classloading issue? Maybe we will not do this.

        DataStream<JsonNode> sourceStream = env.addSource(new FakeKafkaSource(1))
                                               .assignTimestampsAndWatermarks(new MeasurementTSExtractor())
                                               .map(new MeasurementDeserializer());  //ineffecient data type, per-record creation of ObjectMapper, uncaught exception on invalid input

        DataStream<JsonNode> enrichedStream = sourceStream.keyBy(jsonNode -> jsonNode.get("location")
                                                                                     .asText()) // key by location for caching of external IO in EnrichMeasurement
                                                          .map(new EnrichMeasurementByTemperature());


        DataStream<JsonNode> filteredStream = enrichedStream.filter(jsonNode -> jsonNode.has("temperature")) //Unneccessarily late filter -> make EnrichMeasurement a FlatMap
                                                            .map(new MeasurementProjection("temperature", "location", "value")); //Unneccessarily late projection

        DataStream<WindowedMeasurements> avgValuePerLocation = filteredStream.keyBy(jsonNode -> jsonNode.get("location")
                                                                                                        .asText()) //Possibly reinterpretAsKeyedStream
                                                                             .timeWindow(Time.of(10, TimeUnit.SECONDS))
                                                                             .aggregate(new MeasurementAggregationFunction(), new MeasurementWindowFunction()); //never triggered because of idle partitions

        //TODO Maybe replace by StreamingFileSink
        avgValuePerLocation.addSink(new DiscardingSink<>());

        env.execute();
    }

}
