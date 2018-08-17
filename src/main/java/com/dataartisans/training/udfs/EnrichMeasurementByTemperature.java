package com.dataartisans.training.udfs;

import com.dataartisans.training.io.TemperatureClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.Optional;
import java.util.Random;

public class EnrichMeasurementByTemperature extends RichMapFunction<JsonNode, JsonNode> {

    private Random            random;
    private TemperatureClient temperatureClient;

    @Override
    public void open(final Configuration parameters) throws Exception {
        random = new Random();
        temperatureClient = new TemperatureClient();
    }

    @Override
    public JsonNode map(final JsonNode jsonNode) throws Exception {
        Optional<Float> maybeTemperature = temperatureClient.getTemperatureFor(jsonNode.get("location").asText());

        JsonNode enrichedMeasurement = jsonNode;
        if (maybeTemperature.isPresent()) {
            enrichedMeasurement = ((ObjectNode) jsonNode).put("temperature", maybeTemperature.get());
        }
        return enrichedMeasurement;
    }
}
