package com.dataartisans.training.udfs;

import com.dataartisans.training.entities.WindowedMeasurements;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.functions.AggregateFunction;

public class MeasurementAggregationFunction implements AggregateFunction<JsonNode, WindowedMeasurements, WindowedMeasurements> {
    @Override
    public WindowedMeasurements createAccumulator() {
        return new WindowedMeasurements();
    }

    @Override
    public WindowedMeasurements add(final JsonNode value, final WindowedMeasurements accumulator) {
        accumulator.setSumPerWindow(accumulator.getSumPerWindow() + Double.valueOf(value.get("value").asText()));
        return accumulator;
    }

    @Override
    public WindowedMeasurements getResult(final WindowedMeasurements accumulator) {
        return accumulator;
    }

    @Override
    public WindowedMeasurements merge(final WindowedMeasurements a, final WindowedMeasurements b) {
        a.setEventsPerWindow(a.getEventsPerWindow() + b.getEventsPerWindow());
        a.setSumPerWindow((a.getSumPerWindow() + b.getSumPerWindow()));
        return a;
    }
}
