package com.ververica.training.statemigration;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.OutputTag;

import com.ververica.training.DoNotChangeThis;
import com.ververica.training.TroubledStreamingJob;
import com.ververica.training.entities.Measurement;
import com.ververica.training.source.SourceUtils;
import com.ververica.training.udfs.MeasurementTSExtractor;

@DoNotChangeThis
public class StateMigrationJobBase {
    public static final OutputTag<Measurement> LATE_DATA_TAG = new OutputTag<Measurement>("late-data") {
        private static final long serialVersionUID = 33513631677208956L;
    };

    protected static void createAndExecuteJob(
            String[] args,
            SensorAggregationProcessingBase keyedProcessFunction)
            throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        final boolean local = parameters.getBoolean("local", false);

        StreamExecutionEnvironment
                env = TroubledStreamingJob.configuredEnvironment(parameters, local);

        //Time Characteristics
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //Checkpointing Configuration
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4000);

        DataStream<Measurement> sourceStream = env
                .addSource(SourceUtils.createFailureFreeFakeKafkaSource()).name("FakeKafkaSource")
                .assignTimestampsAndWatermarks(new MeasurementTSExtractor())
                .flatMap(new MeasurementDeserializer())
                .name("Deserialization");

        SingleOutputStreamOperator<MeasurementAggregationReport> aggregatedPerSensor = sourceStream
                .keyBy(Measurement::getSensorId)
                .process(keyedProcessFunction)
                .name("AggregatePerSensor (" + keyedProcessFunction.getStateSerializerName() + ")")
                .uid("AggregatePerSensor");

        if (local) {
            aggregatedPerSensor.print().name("NormalOutput").disableChaining();
            aggregatedPerSensor.getSideOutput(LATE_DATA_TAG).addSink(new DiscardingSink<>()).name("LateDataSink").disableChaining();
//            aggregatedPerSensor.getSideOutput(LATE_DATA_TAG).printToErr().name("LateDataSink").disableChaining();
        } else {
            aggregatedPerSensor.addSink(new DiscardingSink<>()).name("NormalOutput").disableChaining();
            aggregatedPerSensor.getSideOutput(LATE_DATA_TAG).addSink(new DiscardingSink<>()).name("LateDataSink").disableChaining();
        }

        env.execute();
    }
}
