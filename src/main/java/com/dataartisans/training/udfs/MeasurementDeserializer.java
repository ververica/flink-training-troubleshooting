package com.dataartisans.training.udfs;

import org.apache.flink.api.common.functions.RichMapFunction;

import com.dataartisans.training.entities.FakeKafkaRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


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
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(bytes, JsonNode.class);
    }

}
