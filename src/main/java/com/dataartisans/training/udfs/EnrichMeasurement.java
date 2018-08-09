package com.dataartisans.training.udfs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.Optional;
import java.util.Random;

public class EnrichMeasurement extends RichMapFunction<JsonNode, JsonNode> {

    private Random random;

    @Override
    public void open(final Configuration parameters) throws Exception {
        random = new Random();
    }

    @Override
    public JsonNode map(final JsonNode jsonNode) throws Exception {
        Optional<Float> maybeTemperature = getTemperatureFor(jsonNode.get("location").asText());

        JsonNode enrichedMeasurement = jsonNode;
        if (maybeTemperature.isPresent()) {
            enrichedMeasurement = ((ObjectNode) jsonNode).put("temperature", maybeTemperature.get());
        }
        return enrichedMeasurement;
    }

    private Optional<Float> getTemperatureFor(String location) throws InterruptedException {

        //TODO: Call external system here

        Thread.sleep(random.nextInt(100));

        if (location.equals("boellenfalltor")) {
            return Optional.of(42.f);
        }
        return Optional.empty();
    }
}
