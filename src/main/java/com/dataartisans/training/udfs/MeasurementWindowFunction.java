package com.dataartisans.training.udfs;

import com.dataartisans.training.entities.WindowedMeasurements;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MeasurementWindowFunction implements WindowFunction<WindowedMeasurements, WindowedMeasurements, String, TimeWindow> {
    @Override
    public void apply(final String locatoin, final TimeWindow window, final Iterable<WindowedMeasurements> input, final Collector<WindowedMeasurements> out) throws Exception {
        WindowedMeasurements aggregate = input.iterator().next();
        aggregate.setWindowStart(window.getStart());
        aggregate.setWindowEnd(window.getEnd());
        aggregate.setLocation(locatoin);
        out.collect(aggregate);
    }
}
