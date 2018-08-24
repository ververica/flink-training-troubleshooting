package com.dataartisans.training.udfs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import com.dataartisans.training.io.TemperatureClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class EnrichMeasurementWithTemperatureAsync extends RichAsyncFunction<JsonNode, JsonNode> {

    private transient TemperatureClient                                                   temperatureClient;
    private transient Map<String, EnrichMeasurementWithTemperature.TemperatureCacheEntry> cache;

    private int     cacheExpiryMs;
    private Counter cacheSizeMetric;
    private Counter servedFromCacheMetric;

    public EnrichMeasurementWithTemperatureAsync(int cacheExpiryMs) {
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
    public void asyncInvoke(final JsonNode jsonNode, final ResultFuture<JsonNode> resultFuture) throws Exception {

        String location = jsonNode.get("location").asText();

        EnrichMeasurementWithTemperature.TemperatureCacheEntry cachedTemperature = cache.get(location);
        if (cachedTemperature != null && !cachedTemperature.isTooOld(cacheExpiryMs)) {
            resultFuture.complete(Collections.singleton(((ObjectNode) jsonNode)
                    .put("temperature", cachedTemperature.getValue())));
            servedFromCacheMetric.inc();
        } else {
            temperatureClient.asyncGetTemperatureFor(jsonNode.get("location")
                                                             .asText(), new TemperatureCallBack(resultFuture, jsonNode, location));
        }


    }

    private class TemperatureCallBack implements Consumer<Float> {
        private final ResultFuture<JsonNode> resultFuture;
        private final JsonNode               jsonNode;
        private final String                 location;

        public TemperatureCallBack(final ResultFuture<JsonNode> resultFuture, final JsonNode jsonNode, final String location) {
            this.resultFuture = resultFuture;
            this.jsonNode = jsonNode;
            this.location = location;
        }

        @Override
        public void accept(final Float temperature) {
            resultFuture.complete(Collections.singleton(((ObjectNode) jsonNode)
                    .put("temperature", temperature)));
            if (cache.put(location, new EnrichMeasurementWithTemperature.TemperatureCacheEntry(System.currentTimeMillis(), temperature)) ==
                null) {
                cacheSizeMetric.inc();
            }
        }
    }
}
