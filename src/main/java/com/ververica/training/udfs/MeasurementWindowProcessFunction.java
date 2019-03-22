package com.ververica.training.udfs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.ververica.training.entities.WindowedMeasurements;

public class MeasurementWindowProcessFunction
        extends ProcessWindowFunction<WindowedMeasurements, WindowedMeasurements, String, TimeWindow> {
    private static final long serialVersionUID = -1083906142198231377L;

    private static final int    EVENT_TIME_LAG_WINDOW_SIZE = 10_000;

    private transient DescriptiveStatisticsHistogram eventTimeLag;

    public MeasurementWindowProcessFunction() {
    }

    @Override
    public void process(
            final String location,
            final Context context,
            final Iterable<WindowedMeasurements> input,
            final Collector<WindowedMeasurements> out) {

        // Windows with pre-aggregation only forward the final to the WindowFunction
        WindowedMeasurements aggregate = input.iterator().next();

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
        eventTimeLag = getRuntimeContext().getMetricGroup().histogram("eventTimeLag",
                new DescriptiveStatisticsHistogram(EVENT_TIME_LAG_WINDOW_SIZE));
    }

}
