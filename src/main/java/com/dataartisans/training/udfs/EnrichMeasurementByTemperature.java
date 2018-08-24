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
import org.apache.flink.metrics.Counter;

import java.util.Map;

public class EnrichMeasurementByTemperature extends RichMapFunction<JsonNode, JsonNode> {

    private transient TemperatureClient                  temperatureClient;
    private transient Map<String, TemperatureCacheEntry> cache;

    private int     cacheExpiryMs;
    private Counter cacheSizeMetric;
    private Counter servedFromCacheMetric;

    public EnrichMeasurementByTemperature(int cacheExpiryMs) {
        this.cacheExpiryMs = cacheExpiryMs;
    }

    @Override
    public void open(final Configuration parameters) throws Exception {
        temperatureClient = new TemperatureClient();
        cache = Maps.newHashMap();
        servedFromCacheMetric = getRuntimeContext().getMetricGroup().counter("temperatureRequestsServedFromCache");
        cacheSizeMetric = getRuntimeContext().getMetricGroup().counter("temperatureCacheSize");
    }

    @Override
    public JsonNode map(final JsonNode jsonNode) throws Exception {

        String location = jsonNode.get("location").asText();

        float temperature;
        TemperatureCacheEntry cachedTemperature = cache.get(location);
        if (cachedTemperature != null && !cachedTemperature.isTooOld(cacheExpiryMs)) {
            temperature = cachedTemperature.getValue();
            servedFromCacheMetric.inc();
        } else {
            temperature = temperatureClient.getTemperatureFor(location);
            if (cache.put(location, new TemperatureCacheEntry(System.currentTimeMillis(), temperature)) == null) {
                cacheSizeMetric.inc();
            }
        }

        return ((ObjectNode) jsonNode).put("temperature", temperature);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TemperatureCacheEntry {
        private long  timestamp;
        private float value;

        public boolean isTooOld(int expiryMs) {
            return System.currentTimeMillis() - timestamp > expiryMs;
        }
    }
}
