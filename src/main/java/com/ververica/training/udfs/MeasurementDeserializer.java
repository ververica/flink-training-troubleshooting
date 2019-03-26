package com.ververica.training.udfs;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;

import com.ververica.training.entities.FakeKafkaRecord;
import com.ververica.training.entities.Measurement;
import com.ververica.training.source.ObjectMapperSingleton;

import java.io.IOException;


/**
 * Deserializes the JSON Kafka message.
 */
public class MeasurementDeserializer extends RichFlatMapFunction<FakeKafkaRecord, Measurement> {
    private static final long serialVersionUID = 4054149949298485680L;

    private Counter numInvalidRecords;

    @Override
    public void open(final Configuration parameters) throws Exception {
        super.open(parameters);
        numInvalidRecords = getRuntimeContext().getMetricGroup().counter("numInvalidRecords");
    }

    @Override
    public void flatMap(final FakeKafkaRecord kafkaRecord, final Collector<Measurement> out) throws Exception {
        try {
            out.collect(deserialize(kafkaRecord.getValue()));
        } catch (IOException e) {
            numInvalidRecords.inc();
        }

    }

    private Measurement deserialize(final byte[] bytes) throws IOException {
        return ObjectMapperSingleton.getInstance().readValue(bytes, Measurement.class);
    }

}
