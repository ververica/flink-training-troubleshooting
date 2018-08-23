package com.dataartisans.training.udfs;

import com.dataartisans.training.io.TemperatureClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.Map;
import java.util.Random;

public class EnrichMeasurementByTemperature extends RichMapFunction<JsonNode, JsonNode> {

    private transient Random                             random;
    private transient TemperatureClient                  temperatureClient;
    private transient Map<String, TemperatureCacheEntry> cache;

    private int cacheExpiryMs;

    public EnrichMeasurementByTemperature(int cacheExpiryMs) {
        this.cacheExpiryMs = cacheExpiryMs;
    }

    @Override
    public void open(final Configuration parameters) throws Exception {
        random = new Random();
        temperatureClient = new TemperatureClient();
        cache = Maps.newHashMap();
    }

    @Override
    public JsonNode map(final JsonNode jsonNode) throws Exception {

        String location = jsonNode.get("location").asText();

        float temperature;
        TemperatureCacheEntry cachedTemperature = cache.get(location);
        if (cachedTemperature != null && !cachedTemperature.isTooOld(cacheExpiryMs)) {
            temperature = cachedTemperature.getValue();
        } else {
            temperature = temperatureClient.getTemperatureFor(location);
        }

        return ((ObjectNode) jsonNode).put("temperature", temperature);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class TemperatureCacheEntry {
        private long  timestamp;
        private float value;

        private boolean isTooOld(int expiryMs) {
            return System.currentTimeMillis() - timestamp > expiryMs;
        }
    }
}
