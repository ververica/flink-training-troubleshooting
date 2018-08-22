package com.dataartisans.training.udfs;

import com.dataartisans.training.entities.FakeKafkaRecord;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

public class MeasurementTSExtractor extends BoundedOutOfOrdernessTimestampExtractor<FakeKafkaRecord> {
    public MeasurementTSExtractor() {
        super(Time
                .of(100, TimeUnit.MILLISECONDS));
    }

    @Override
    public long extractTimestamp(final FakeKafkaRecord record) {
        return record.getTimestamp();
    }
}
