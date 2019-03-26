package com.ververica.training.udfs;

import org.apache.flink.api.common.functions.AggregateFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.ververica.training.DoNotChangeThis;
import com.ververica.training.entities.Measurement;
import com.ververica.training.entities.WindowedMeasurements;
import org.apache.commons.math3.util.CombinatoricsUtils;

public class MeasurementWindowAggregatingFunction
        implements AggregateFunction<Measurement, WindowedMeasurements, WindowedMeasurements> {
    private static final long serialVersionUID = -1083906142198231377L;

    public MeasurementWindowAggregatingFunction() {}

    @Override
    public WindowedMeasurements createAccumulator() {
        return new WindowedMeasurements();
    }

    @Override
    public WindowedMeasurements add(final Measurement record, final WindowedMeasurements aggregate) {
        double result = record.getValue();
        aggregate.setSumPerWindow(aggregate.getSumPerWindow() + result);
        aggregate.setEventsPerWindow(aggregate.getEventsPerWindow() + 1);
        return aggregate;
    }

    @Override
    public WindowedMeasurements getResult(final WindowedMeasurements windowedMeasurements) {
        return windowedMeasurements;
    }

    @Override
    public WindowedMeasurements merge(final WindowedMeasurements agg1, final WindowedMeasurements agg2) {
        agg2.setEventsPerWindow(agg1.getEventsPerWindow() + agg2.getEventsPerWindow());
        agg2.setSumPerWindow(agg1.getSumPerWindow() + agg2.getSumPerWindow());
        return agg2;
    }

}
