package com.ververica.training.udfs;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.ververica.training.entities.FakeKafkaRecord;

import java.util.concurrent.TimeUnit;

public class MeasurementTSExtractor implements AssignerWithPeriodicWatermarks<FakeKafkaRecord> {

    private long currentMaxTimestamp;
    private long lastEmittedWatermark = Long.MIN_VALUE;
    private long lastRecordProcessingTime;

    private final long maxOutOfOrderness;
    private final long idleTimeout;


    public MeasurementTSExtractor() {
        this(Time.of(250, TimeUnit.MILLISECONDS), Time.of(1, TimeUnit.SECONDS));
    }

    public MeasurementTSExtractor(Time maxOutOfOrderness, Time idleTimeout) {
        if (maxOutOfOrderness.toMilliseconds() < 0) {
            throw new RuntimeException("Tried to set the maximum allowed " +
                                       "lateness to " + maxOutOfOrderness + ". This parameter cannot be negative.");
        }

        if (idleTimeout.toMilliseconds() < 0) {
            throw new RuntimeException("Tried to set the idle Timeout" + idleTimeout + ". This parameter cannot be negative.");
        }


        this.maxOutOfOrderness = maxOutOfOrderness.toMilliseconds();
        this.idleTimeout = idleTimeout.toMilliseconds();
        this.currentMaxTimestamp = Long.MIN_VALUE;
    }

    public long getMaxOutOfOrdernessInMillis() {
        return maxOutOfOrderness;
    }

    @Override
    public final Watermark getCurrentWatermark() {

        //if last record was processed more than the idleTimeout in the past, consider this source idle and set timestamp to current processing time
        long currentProcessingTime = System.currentTimeMillis();
        if (lastRecordProcessingTime < currentProcessingTime - idleTimeout) {
            this.currentMaxTimestamp = currentProcessingTime;
        }

        long potentialWM = this.currentMaxTimestamp - maxOutOfOrderness;
        if (potentialWM >= lastEmittedWatermark) {
            lastEmittedWatermark = potentialWM;
        }
        return new Watermark(lastEmittedWatermark);
    }

    @Override
    public final long extractTimestamp(FakeKafkaRecord element, long previousElementTimestamp) {
        lastRecordProcessingTime = System.currentTimeMillis();
        long timestamp = element.getTimestamp();
        if (timestamp > currentMaxTimestamp) {
            currentMaxTimestamp = timestamp;
        }
        return timestamp;
    }
}