package com.dataartisans.training.udfs;

import org.apache.flink.api.common.functions.RichMapFunction;

import com.dataartisans.training.entities.FakeKafkaRecord;
import com.dataartisans.training.source.ObjectMapperSingleton;
import com.fasterxml.jackson.databind.JsonNode;


/**
 * Deserializes the JSON Kafka message.
 */
public class MeasurementDeserializer extends RichMapFunction<FakeKafkaRecord, JsonNode> {

    @Override
    public JsonNode map(final FakeKafkaRecord kafkaRecord) throws Exception {
        return deserialize(kafkaRecord.getValue());
    }

    private JsonNode deserialize(final byte[] bytes) throws java.io.IOException {
        return ObjectMapperSingleton.getInstance().readValue(bytes, JsonNode.class);
    }

}
