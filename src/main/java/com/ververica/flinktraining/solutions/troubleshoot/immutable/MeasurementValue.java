package com.ververica.flinktraining.solutions.troubleshoot.immutable;

import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.reflect.Type;
import java.util.Map;

@TypeInfo(MeasurementValue.MeasurementValueTypeInfoFactory.class)
public class MeasurementValue {
    private double value;
    private float accuracy;
    private long timestamp;

    public MeasurementValue(double value, float accuracy, long timestamp) {
        this.value = value;
        this.accuracy = accuracy;
        this.timestamp = timestamp;
    }

    public double getValue() {
        return value;
    }

    public float getAccuracy() {
        return accuracy;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public static class MeasurementValueTypeInfoFactory extends TypeInfoFactory<MeasurementValue> {
        @Override
        public TypeInformation<MeasurementValue> createTypeInfo(
                Type t,
                Map<String, TypeInformation<?>> genericParameters) {
            return MeasurementValueTypeInfo.INSTANCE;
        }
    }
}
