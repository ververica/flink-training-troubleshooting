package com.ververica.training;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.OutputTag;

import com.ververica.training.entities.WindowedMeasurements;
import com.ververica.training.source.SourceUtils;
import com.ververica.training.udfs.MeasurementDeserializer;
import com.ververica.training.udfs.MeasurementTSExtractor;
import com.ververica.training.udfs.MeasurementWindowAggregatingFunction;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

public class TroubledStreamingJob {

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        final boolean local = parameters.getBoolean("local", false);

        StreamExecutionEnvironment env = configuredEnvironment(parameters, local);

        //Time Characteristics
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(2000);

        //Checkpointing Configuration
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4000);

        DataStream<JsonNode> sourceStream = env
                .addSource(SourceUtils.createFakeKafkaSource())
                .name("FakeKafkaSource")
                .uid("FakeKafkaSource")
                .assignTimestampsAndWatermarks(new MeasurementTSExtractor())
                .name("Watermarks")
                .uid("Watermarks")
                .map(new MeasurementDeserializer())
                .name("Deserialization")
                .uid("Deserialization");

        OutputTag<JsonNode> lateDataTag = new OutputTag<JsonNode>("late-data") {
            private static final long serialVersionUID = 33513631677208956L;
        };

        SingleOutputStreamOperator<WindowedMeasurements> aggregatedPerLocation = sourceStream
                .keyBy(jsonNode -> jsonNode.get("location").asText())
                .timeWindow(Time.of(1, TimeUnit.SECONDS))
                .sideOutputLateData(lateDataTag)
                .process(new MeasurementWindowAggregatingFunction())
                .name("WindowedAggregationPerLocation")
                .uid("WindowedAggregationPerLocation");

        if (local) {
            aggregatedPerLocation.print()
                    .name("NormalOutput")
                    .uid("NormalOutput")
                    .disableChaining();
            aggregatedPerLocation.getSideOutput(lateDataTag).printToErr()
                    .name("LateDataSink")
                    .uid("LateDataSink")
                    .disableChaining();
        } else {
            aggregatedPerLocation.addSink(new DiscardingSink<>())
                    .name("NormalOutput")
                    .uid("NormalOutput")
                    .disableChaining();
            aggregatedPerLocation.getSideOutput(lateDataTag).addSink(new DiscardingSink<>())
                    .name("LateDataSink")
                    .uid("LateDataSink")
                    .disableChaining();
        }

        env.execute();
    }

    public static StreamExecutionEnvironment configuredEnvironment(final ParameterTool parameters, final boolean local) throws IOException, URISyntaxException {
        StreamExecutionEnvironment env;
        if (local) {
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

            String statePath = parameters.get("fsStatePath");
            Path checkpointPath;
            if (statePath != null) {
                FileUtils.deleteDirectory(new File(new URI(statePath)));
                checkpointPath = Path.fromLocalFile(new File(new URI(statePath)));
            } else {
                checkpointPath = Path.fromLocalFile(Files.createTempDirectory("checkpoints").toFile());
            }

            StateBackend stateBackend = new FsStateBackend(checkpointPath);
            env.setStateBackend(stateBackend);
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }

        env.getConfig().setGlobalJobParameters(parameters);
        return env;
    }
}
