package com.dataartisans.training.udfs;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.dataartisans.training.entities.FakeKafkaRecord;

import java.util.concurrent.TimeUnit;

public class MeasurementTSExtractor extends BoundedOutOfOrdernessTimestampExtractor<FakeKafkaRecord> {
    private static final long serialVersionUID = 1162153268960980262L;

    public MeasurementTSExtractor() {
        super(Time.of(100, TimeUnit.MILLISECONDS));
    }

    @Override
    public long extractTimestamp(final FakeKafkaRecord record) {
        return record.getTimestamp();
    }
}
