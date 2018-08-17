package com.dataartisans.training;

import com.dataartisans.training.entities.FakeKafkaRecord;
import com.dataartisans.training.source.FakeKafkaSource;
import com.dataartisans.training.udfs.MeasurementDeserializer;
import com.dataartisans.training.udfs.MeasurementProjection;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

public class TroubledStreamingJob {

    public static void main(String[] args) throws Exception {

        //TODO change back to getExecutionEnvironment()
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setLatencyTrackingInterval(100);
        env.enableCheckpointing(1000);


        //TODO Classloading issue

        //FakeKafkaSource already assigns Timestamps and Watermarks
        SingleOutputStreamOperator<JsonNode> measurements = env.addSource(new FakeKafkaSource(1))
                                                               .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<FakeKafkaRecord>(Time
                                                                       .of(100, TimeUnit.MILLISECONDS)) {
                                                                   @Override
                                                                   public long extractTimestamp(final FakeKafkaRecord record) {
                                                                       return record.getTimestamp();
                                                                   }
                                                               })
                                                               .rebalance()
                                                               .map(new MeasurementDeserializer()) //ineffecient data type, per-record creation of ObjectMapper, uncaught exception on invalid input
                                                               .filter(jsonNode -> jsonNode.has("temperature")) //Unneccessarily late filter -> make EnrichMeasurement a FlatMap
                                                               .map(new MeasurementProjection("temperature", "location", "value")); //Unneccessarily late projection


        //TODO partitioning, maybe some state cleanup issues or window

        measurements.addSink(new DiscardingSink<>());

        env.execute();
    }

}
