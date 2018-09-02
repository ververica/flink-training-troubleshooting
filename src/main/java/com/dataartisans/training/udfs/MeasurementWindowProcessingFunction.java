/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.training.udfs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.dataartisans.training.entities.WindowedMeasurements;

public class MeasurementWindowProcessingFunction
        extends ProcessWindowFunction<WindowedMeasurements, WindowedMeasurements, String, TimeWindow> {
    private static final long serialVersionUID = 961490500466491714L;

    private static final int EVENT_TIME_LAG_WINDOW_SIZE = 250_000;

    private transient DescriptiveStatisticsHistogram eventTimeLag;

    @Override
    public void process(
            final String location,
            final Context context,
            final Iterable<WindowedMeasurements> input,
            final Collector<WindowedMeasurements> out) {

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
