package com.dataartisans.training.udfs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.dataartisans.training.entities.WindowedMeasurements;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.math3.util.ArithmeticUtils;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.apache.commons.math3.util.MathUtils;

public class MeasurementWindowAggregatingFunction
        extends ProcessWindowFunction<JsonNode, WindowedMeasurements, String, TimeWindow> {
    private static final long serialVersionUID = -1083906142198231377L;

    private DescriptiveStatisticsHistogram eventTimeLag;

    @Override
    public void process(
            final String location,
            final Context context,
            final Iterable<JsonNode> input,
            final Collector<WindowedMeasurements> out) {

        WindowedMeasurements aggregate = new WindowedMeasurements();
        for (JsonNode record : input) {
            double result = doComplexCalculation(Double.valueOf(record.get("value").asText()));
            aggregate.setSumPerWindow(aggregate.getSumPerWindow() + result);
            aggregate.setEventsPerWindow(aggregate.getEventsPerWindow() + 1);
        }

        final TimeWindow window = context.window();
        aggregate.setWindowStart(window.getStart());
        aggregate.setWindowEnd(window.getEnd());
        aggregate.setLocation(location);

        eventTimeLag.update(System.currentTimeMillis() - window.getEnd());
        out.collect(aggregate);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        eventTimeLag = getRuntimeContext().getMetricGroup().histogram("eventTimeLag", new DescriptiveStatisticsHistogram(250_000));
    }

    // ------------------------------------------------------------------------

    /**
     * Simulates some more or less complex calculation done per element.
     *
     * <p>Take this calculation as a needed part of the application logic that returns some unique
     * value for each input and cannot be cached.
     */
    private static double doComplexCalculation(Double doubleValue) {
        return CombinatoricsUtils.factorialDouble((int) (100 * doubleValue));
    }
}
