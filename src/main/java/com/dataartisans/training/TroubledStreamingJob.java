package com.dataartisans.training;

import com.dataartisans.training.source.FakeKafkaSource;
import com.dataartisans.training.udfs.EnrichMeasurement;
import com.dataartisans.training.udfs.MeasurementDeserializer;
import com.dataartisans.training.udfs.MeasurementProjection;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

public class TroubledStreamingJob {

    public static void main(String[] args) throws Exception {

        //TODO change back to getExecutionEnvironment()
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        env.getConfig().setLatencyTrackingInterval(100);

        env.enableCheckpointing(1000);

        env.setStateBackend(new FsStateBackend(new Path("file:/tmp/flink-checkpoints")));

        //FakeKafkaSource already assigns Timestamps and Watermarks
        DataStream<JsonNode> measurements = env.addSource(new FakeKafkaSource(1000))
                                               .map(new MeasurementDeserializer()) //ineffecient data type, per-record creation of ObjectMapper, uncaught exception on invalid input
                                               .map(new EnrichMeasurement()) // Make AsyncIO
                                               .filter(jsonNode -> jsonNode.has("temperature")) //Unneccessarily late filter -> make EnrichMeasurement a FlatMap
                                               .map(new MeasurementProjection("temperature", "location", "value")); //Unneccessarily late projection


        //TODO partitioning, maybe some state cleanup issues or window

        measurements.addSink(new DiscardingSink<>());

        env.execute();
    }

}
