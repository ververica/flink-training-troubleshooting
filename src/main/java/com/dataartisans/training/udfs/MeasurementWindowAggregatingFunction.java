package com.dataartisans.training.udfs;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.utils.ParameterTool;

import com.dataartisans.training.DoNotChangeThis;
import com.dataartisans.training.entities.Measurement;
import com.dataartisans.training.entities.WindowedMeasurements;
import org.apache.commons.math3.util.CombinatoricsUtils;

public class MeasurementWindowAggregatingFunction
        implements AggregateFunction<Measurement, WindowedMeasurements, WindowedMeasurements> {
    private static final long serialVersionUID = -1083906142198231377L;

    private final boolean doHeavyComputation;

    public MeasurementWindowAggregatingFunction(ParameterTool jobParameters) {
        this.doHeavyComputation = jobParameters.has("latencyUseCase");
    }

    @Override
    public WindowedMeasurements createAccumulator() {
        return new WindowedMeasurements();
    }

    @Override
    public WindowedMeasurements add(final Measurement record, final WindowedMeasurements aggregate) {
        double result = calculate(record.getValue(), (double) record.getTemperature());
        aggregate.setSumPerWindow(aggregate.getSumPerWindow() + result);
        aggregate.setEventsPerWindow(aggregate.getEventsPerWindow() + 1);
        return aggregate;
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

    // ------------------------------------------------------------------------

    /**
     * Simulates some more or less complex calculation done per element.
     *
     * <p>Take this calculation as a needed part of the application logic that returns some unique
     * value for each input and cannot be cached.
     */
    @DoNotChangeThis
    private double calculate(Double doubleValue, final Double temperature) {
        if (doHeavyComputation) {
            long startTime = System.nanoTime();
            CombinatoricsUtils.factorialDouble((int) (100 * Math.max(doubleValue, temperature)));
            return System.nanoTime() - startTime;
        } else {
            return Math.max(doubleValue, temperature);
        }
    }
}
