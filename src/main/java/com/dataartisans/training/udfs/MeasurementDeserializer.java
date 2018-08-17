package com.dataartisans.training.udfs;

import com.dataartisans.training.entities.FakeKafkaRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;


/**
 * Deserializes the JSON Kafka message.
 */
public class MeasurementDeserializer extends RichMapFunction<FakeKafkaRecord, JsonNode> {

    @Override
    public JsonNode map(final FakeKafkaRecord kafkaRecord) throws Exception {
        return deserialize(kafkaRecord.getValue());
    }

    private JsonNode deserialize(final byte[] bytes) throws java.io.IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(bytes, JsonNode.class);
    }

}
