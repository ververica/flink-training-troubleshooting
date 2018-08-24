package com.dataartisans.training.udfs;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;

import com.dataartisans.training.entities.FakeKafkaRecord;
import com.dataartisans.training.source.ObjectMapperSingleton;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;


/**
 * Deserializes the JSON Kafka message.
 */
public class MeasurementDeserializer extends RichFlatMapFunction<FakeKafkaRecord, JsonNode> {
    private static final long serialVersionUID = 4054149949298485680L;

    private Counter deserializationFailures;

    @Override
    public void open(final Configuration parameters) throws Exception {
        super.open(parameters);
        deserializationFailures = getRuntimeContext().getMetricGroup().counter("deserializationFailures");
    }

    private JsonNode deserialize(final byte[] bytes) throws java.io.IOException {
        return ObjectMapperSingleton.getInstance().readValue(bytes, JsonNode.class);
    }

    @Override
    public void flatMap(final FakeKafkaRecord kafkaRecord, final Collector<JsonNode> out) throws Exception {
        try {
            JsonNode jsonNode = deserialize(kafkaRecord.getValue());
            out.collect(jsonNode);
        } catch (IOException e) {
            deserializationFailures.inc();
        }
    }
}
