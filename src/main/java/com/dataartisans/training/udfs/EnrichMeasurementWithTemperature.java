package com.dataartisans.training.udfs;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

import com.dataartisans.training.io.TemperatureClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class EnrichMeasurementWithTemperature extends RichMapFunction<JsonNode, JsonNode> {
    private static final long serialVersionUID = -4682640428731250473L;

    private transient TemperatureClient                  temperatureClient;
    private transient Map<String, TemperatureCacheEntry> cache;

    private final int     cacheExpiryMs;
    private       Counter cacheSizeMetric;
    private       Counter servedFromCacheMetric;

    public EnrichMeasurementWithTemperature(int cacheExpiryMs) {
        this.cacheExpiryMs = cacheExpiryMs;
    }

    @Override
    public void open(final Configuration parameters) throws Exception {
        temperatureClient = new TemperatureClient();
        cache = new HashMap<>();
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

    public static class TemperatureCacheEntry {
        private long  timestamp;
        private float value;

        public boolean isTooOld(int expiryMs) {
            return System.currentTimeMillis() - timestamp >= expiryMs;
        }

        public TemperatureCacheEntry() {
        }

        public TemperatureCacheEntry(final long timestamp, final float value) {
            this.timestamp = timestamp;
            this.value = value;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(final long timestamp) {
            this.timestamp = timestamp;
        }

        public float getValue() {
            return value;
        }

        public void setValue(final float value) {
            this.value = value;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final TemperatureCacheEntry that = (TemperatureCacheEntry) o;
            return timestamp == that.timestamp &&
                   Float.compare(that.value, value) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, value);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("TemperatureCacheEntry{");
            sb.append("timestamp=").append(timestamp);
            sb.append(", value=").append(value);
            sb.append('}');
            return sb.toString();
        }
    }
}
