package com.ververica.training.udfs;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.ververica.training.entities.FakeKafkaRecord;

import java.util.concurrent.TimeUnit;

public class MeasurementTSExtractor extends BoundedOutOfOrdernessTimestampExtractor<FakeKafkaRecord> {
    private static final long serialVersionUID = 1162153268960980262L;

    public MeasurementTSExtractor() {
        super(Time.of(250, TimeUnit.MILLISECONDS));
    }

    @Override
    public long extractTimestamp(final FakeKafkaRecord record) {
        return record.getTimestamp();
    }
}
