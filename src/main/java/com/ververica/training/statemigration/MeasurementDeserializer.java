package com.ververica.training.statemigration;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.training.DoNotChangeThis;
import com.ververica.training.entities.FakeKafkaRecord;
import com.ververica.training.entities.Measurement;
import com.ververica.training.source.ObjectMapperSingleton;

import java.io.IOException;

/**
 * Deserializes {@link FakeKafkaRecord} into {@link Measurement} objects, ignoring deserialization
 * failures.
 */
@DoNotChangeThis
public class MeasurementDeserializer extends RichFlatMapFunction<FakeKafkaRecord, Measurement> {

    private static final long serialVersionUID = -5805258552949837150L;

    private transient ObjectMapper mapper;

    @Override
    public void open(final Configuration parameters) throws Exception {
        super.open(parameters);
        mapper = ObjectMapperSingleton.getInstance();
    }

    private Measurement deserialize(final byte[] bytes) throws IOException {
        return mapper.readValue(bytes, Measurement.class);
    }

    @Override
    public void flatMap(final FakeKafkaRecord kafkaRecord, final Collector<Measurement> out) {
        try {
            out.collect(deserialize(kafkaRecord.getValue()));
        } catch (IOException ignored) {
        }
    }
}
