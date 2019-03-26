package com.ververica.training.udfs;

import org.apache.flink.api.common.functions.RichMapFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.ververica.training.entities.FakeKafkaRecord;
import com.ververica.training.source.ObjectMapperSingleton;


/**
 * Deserializes the JSON Kafka message.
 */
public class MeasurementDeserializer extends RichMapFunction<FakeKafkaRecord, JsonNode> {
    private static final long serialVersionUID = 4054149949298485680L;

    @Override
    public JsonNode map(final FakeKafkaRecord kafkaRecord) throws Exception {
        return deserialize(kafkaRecord.getValue());
    }

    private JsonNode deserialize(final byte[] bytes) throws java.io.IOException {
        return ObjectMapperSingleton.getInstance().readValue(bytes, JsonNode.class);
    }

}
