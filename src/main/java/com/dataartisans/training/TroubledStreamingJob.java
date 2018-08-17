package com.dataartisans.training;

import com.dataartisans.training.entities.FakeKafkaRecord;
import com.dataartisans.training.entities.WindowedMeasurements;
import com.dataartisans.training.source.FakeKafkaSource;
import com.dataartisans.training.udfs.EnrichMeasurementByTemperature;
import com.dataartisans.training.udfs.MeasurementDeserializer;
import com.dataartisans.training.udfs.MeasurementProjection;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class TroubledStreamingJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(500);

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
                                                               .map(new MeasurementDeserializer()) //ineffecient data type, per-record creation of ObjectMapper, uncaught exception on invalid input
                                                               .keyBy(jsonNode -> jsonNode.get("location")
                                                                                          .asText()) // key by location for caching of external IO in EnrichMeasurement
                                                               .map(new EnrichMeasurementByTemperature())
                                                               .filter(jsonNode -> jsonNode.has("temperature")) //Unneccessarily late filter -> make EnrichMeasurement a FlatMap
                                                               .map(new MeasurementProjection("temperature", "location", "value")); //Unneccessarily late projection


        DataStream<WindowedMeasurements> avgValuePerLocation = measurements.keyBy(jsonNode -> jsonNode.get("location")
                                                                                                      .asText()) //Possibly reinterpretAsKeyedStream
                                                                           .timeWindow(Time.of(10, TimeUnit.SECONDS))
                                                                           .aggregate(new MeasurementAggregationFunction(), new MeasurementWindowFunction()); //never triggered because of idle partitions

        //TODO Maybe replace by StreamingFileSink
        avgValuePerLocation.addSink(new DiscardingSink<>());

        measurements.addSink(new DiscardingSink<>());

        env.execute();
    }

    private static class MeasurementAggregationFunction implements AggregateFunction<JsonNode, WindowedMeasurements, WindowedMeasurements> {
        @Override
        public WindowedMeasurements createAccumulator() {
            return new WindowedMeasurements();
        }

        @Override
        public WindowedMeasurements add(final JsonNode value, final WindowedMeasurements accumulator) {
            accumulator.setSumPerWindow(accumulator.getSumPerWindow() + Double.valueOf(value.get("value").asText()));
            return accumulator;
        }

        @Override
        public WindowedMeasurements getResult(final WindowedMeasurements accumulator) {
            return accumulator;
        }

        @Override
        public WindowedMeasurements merge(final WindowedMeasurements a, final WindowedMeasurements b) {
            a.setEventsPerWindow(a.getEventsPerWindow() + b.getEventsPerWindow());
            a.setSumPerWindow((a.getSumPerWindow() + b.getSumPerWindow()));
            return a;
        }
    }

    private static class MeasurementWindowFunction implements WindowFunction<WindowedMeasurements, WindowedMeasurements, String, TimeWindow> {
        @Override
        public void apply(final String locatoin, final TimeWindow window, final Iterable<WindowedMeasurements> input, final Collector<WindowedMeasurements> out) throws Exception {
            WindowedMeasurements aggregate = input.iterator().next();
            aggregate.setWindowStart(window.getStart());
            aggregate.setWindowEnd(window.getEnd());
            aggregate.setLocation(locatoin);
            out.collect(aggregate);
        }
    }
}
