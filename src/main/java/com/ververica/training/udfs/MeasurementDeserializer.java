package com.ververica.training.udfs;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.training.entities.FakeKafkaRecord;
import com.ververica.training.entities.Measurement;
import com.ververica.training.entities.SimpleMeasurement;
import com.ververica.training.source.ObjectMapperSingleton;

import java.io.IOException;


/**
 * Deserializes the JSON Kafka message.
 */
public class MeasurementDeserializer extends RichFlatMapFunction<FakeKafkaRecord, SimpleMeasurement> {
    private static final long serialVersionUID = 4054149949298485680L;

    private Counter numInvalidRecords;
    private transient ObjectMapper instance;

    @Override
    public void open(final Configuration parameters) throws Exception {
        super.open(parameters);
        numInvalidRecords = getRuntimeContext().getMetricGroup().counter("numInvalidRecords");
        instance = ObjectMapperSingleton.getInstance();
    }

    @Override
    public void flatMap(final FakeKafkaRecord kafkaRecord, final Collector<SimpleMeasurement> out) throws Exception {
        try {
            out.collect(deserialize(kafkaRecord.getValue()));
        } catch (IOException e) {
            numInvalidRecords.inc();
        }

    }

    private SimpleMeasurement deserialize(final byte[] bytes) throws IOException {
        return instance.readValue(bytes, SimpleMeasurement.class);
    }

}
