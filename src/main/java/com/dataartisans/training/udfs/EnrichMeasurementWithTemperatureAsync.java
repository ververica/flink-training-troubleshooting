package com.dataartisans.training.udfs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import com.dataartisans.training.entities.Measurement;
import com.dataartisans.training.io.TemperatureClient;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class EnrichMeasurementWithTemperatureAsync extends RichAsyncFunction<Measurement, Measurement> {

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
    public void asyncInvoke(final Measurement measurement, final ResultFuture<Measurement> resultFuture) throws Exception {

        String location = measurement.getLocation();

        EnrichMeasurementWithTemperature.TemperatureCacheEntry cachedTemperature = cache.get(location);
        if (cachedTemperature != null && !cachedTemperature.isTooOld(cacheExpiryMs)) {
            measurement.setTemperature(cachedTemperature.getValue());
            resultFuture.complete(Collections.singleton((measurement)));
            servedFromCacheMetric.inc();
        } else {
            temperatureClient.asyncGetTemperatureFor(measurement.getLocation(), new TemperatureCallBack(resultFuture, measurement, location));
        }


    }

    private class TemperatureCallBack implements Consumer<Float> {
        private final ResultFuture<Measurement> resultFuture;
        private final Measurement               measurement;
        private final String                    location;

        public TemperatureCallBack(final ResultFuture<Measurement> resultFuture, final Measurement measurement, final String location) {
            this.resultFuture = resultFuture;
            this.measurement = measurement;
            this.location = location;
        }

        @Override
        public void accept(final Float temperature) {
            measurement.setTemperature(temperature);
            resultFuture.complete(Collections.singleton((measurement)));
            if (cache.put(location, new EnrichMeasurementWithTemperature.TemperatureCacheEntry(System.currentTimeMillis(), temperature)) ==
                null) {
                cacheSizeMetric.inc();
            }
        }
    }
}
