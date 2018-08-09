package com.dataartisans.training.udfs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;


/**
 * Deserializes the JSON Kafka message.
 */
public class MeasurementDeserializer extends RichMapFunction<byte[], JsonNode> {

    @Override
    public JsonNode map(final byte[] bytes) throws Exception {
        return deserialize(bytes);
    }

    private JsonNode deserialize(final byte[] bytes) throws java.io.IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(bytes, JsonNode.class);
    }

}
