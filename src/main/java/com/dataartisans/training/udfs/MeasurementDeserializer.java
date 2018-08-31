package com.dataartisans.training.udfs;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;

import com.dataartisans.training.entities.FakeKafkaRecord;
import com.dataartisans.training.entities.Measurement;
import com.dataartisans.training.source.ObjectMapperSingleton;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;


/**
 * Deserializes the JSON Kafka message.
 */
public class MeasurementDeserializer extends RichFlatMapFunction<FakeKafkaRecord, Measurement> {
    private static final long serialVersionUID = 4054149949298485680L;

    private Counter      deserializationFailures;
    private ObjectMapper mapper;

    @Override
    public void open(final Configuration parameters) throws Exception {
        super.open(parameters);
        deserializationFailures = getRuntimeContext().getMetricGroup().counter("deserializationFailures");
        mapper = ObjectMapperSingleton.getInstance();
    }

    private Measurement deserialize(final byte[] bytes) throws java.io.IOException {
        return mapper.readValue(bytes, Measurement.class);
    }

    @Override
    public void flatMap(final FakeKafkaRecord kafkaRecord, final Collector<Measurement> out) throws Exception {
        try {
            Measurement jsonNode = deserialize(kafkaRecord.getValue());
            out.collect(jsonNode);
        } catch (IOException e) {
            deserializationFailures.inc();
        }
    }
}
