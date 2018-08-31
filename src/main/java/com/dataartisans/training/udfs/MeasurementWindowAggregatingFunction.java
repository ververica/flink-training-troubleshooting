package com.dataartisans.training.udfs;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.dataartisans.training.DoNotChangeThis;
import com.dataartisans.training.entities.Measurement;
import com.dataartisans.training.entities.WindowedMeasurements;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MeasurementWindowAggregatingFunction
        extends ProcessWindowFunction<Measurement, WindowedMeasurements, String, TimeWindow> {
    private static final long serialVersionUID = -1083906142198231377L;

    public static final  Logger log                        = LoggerFactory.getLogger(MeasurementWindowAggregatingFunction.class);
    private static final int    EVENT_TIME_LAG_WINDOW_SIZE = 250_000;

    private transient DescriptiveStatisticsHistogram eventTimeLag;

    private transient boolean doHeavyComputation;

    public MeasurementWindowAggregatingFunction() {
    }

    @Override
    public void process(
            final String location,
            final Context context,
            final Iterable<Measurement> input,
            final Collector<WindowedMeasurements> out) {

        WindowedMeasurements aggregate = new WindowedMeasurements();
        for (Measurement record : input) {
            double result = calculate(Double.valueOf(record.getValue()), Double.valueOf(record.getTemperature()));
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

        ParameterTool jobParameters =
                (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        doHeavyComputation = jobParameters.has("latencyUseCase");

        eventTimeLag = getRuntimeContext().getMetricGroup().histogram("eventTimeLag",
                new DescriptiveStatisticsHistogram(EVENT_TIME_LAG_WINDOW_SIZE));

        log.info("Initialised window function (heavy computation: {})", doHeavyComputation);
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
