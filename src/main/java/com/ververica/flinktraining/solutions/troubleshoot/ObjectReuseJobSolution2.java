package com.ververica.flinktraining.solutions.troubleshoot;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Collector;

import com.ververica.flinktraining.provided.troubleshoot.GeoUtils;
import com.ververica.flinktraining.provided.troubleshoot.MeanGauge;
import com.ververica.flinktraining.provided.troubleshoot.WeatherUtils;
import com.ververica.flinktraining.solutions.troubleshoot.immutable.ExtendedMeasurement;
import com.ververica.flinktraining.solutions.troubleshoot.immutable.Location;
import com.ververica.flinktraining.solutions.troubleshoot.immutable.MeasurementValue;
import com.ververica.flinktraining.solutions.troubleshoot.immutable.ObjectReuseExtendedMeasurementSource;
import com.ververica.flinktraining.solutions.troubleshoot.immutable.Sensor;

import java.util.HashMap;
import java.util.Map;

import static com.ververica.flinktraining.exercises.troubleshoot.ObjectReuseJobUtils.createConfiguredEnvironment;

public class ObjectReuseJobSolution2 {

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        final boolean local = parameters.getBoolean("local", false);

        StreamExecutionEnvironment env = createConfiguredEnvironment(parameters, local);

        final boolean objectReuse = parameters.getBoolean("objectReuse", false);
        if (objectReuse) {
            env.getConfig().enableObjectReuse();
        }

        //Checkpointing Configuration
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4000);

        SingleOutputStreamOperator<ExtendedMeasurement> temperatureStream = env
                .addSource(new ObjectReuseExtendedMeasurementSource())
                .name("FakeMeasurementSource")
                .uid("FakeMeasurementSource")
                .filter(t -> t.getSensor().getSensorType().equals(Sensor.SensorType.Temperature))
                .name("FilterTemperature")
                .uid("FilterTemperature");

        temperatureStream
                .map(new ConvertToLocalTemperature())
                .name("ConvertToLocalTemperature")
                .uid("ConvertToLocalTemperature")
                .addSink(new DiscardingSink<>())
                .name("LocalizedTemperatureSink")
                .uid("LocalizedTemperatureSink")
                .disableChaining();

        temperatureStream
                .flatMap(new MovingAverageSensors())
                .name("MovingAverageTemperature")
                .uid("MovingAverageTemperature")
                .map(new ConvertToLocalTemperature())
                .name("ConvertToLocalAverageTemperature")
                .uid("ConvertToLocalAverageTemperature")
                .addSink(new DiscardingSink<>())
                .name("LocalizedAverageTemperatureSink")
                .uid("LocalizedAverageTemperatureSink")
                .disableChaining();

        env.execute(ObjectReuseJobSolution2.class.getSimpleName());
    }

    /**
     * Implements an exponentially moving average with a coefficient of <code>0.5</code>, i.e.
     * <ul>
     *     <li><code>avg[0] = value[0]</code> (not forwarded to the next stream)</li>
     *     <li><code>avg[i] = avg[i-1] * 0.5 + value[i] * 0.5</code> (for <code>i > 0</code>)</li>
     * </ul>
     *
     * See <a href="https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">
     *     https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average</a>
     */
    private static class MovingAverageSensors extends
            RichFlatMapFunction<ExtendedMeasurement, ExtendedMeasurement> {
        private static final long serialVersionUID = 1L;

        private Map<Sensor, Tuple2<Float, Double>> lastAverage = new HashMap<>();

        @Override
        public void flatMap(ExtendedMeasurement value, Collector<ExtendedMeasurement> out) {
            Sensor sensor = value.getSensor();

            Tuple2<Float, Double> last = lastAverage.get(sensor);
            if (last != null) {
                float newAccuracy = (last.f0 + value.getMeasurement().getAccuracy()) / 2.0f;
                double newValue = (last.f1 + value.getMeasurement().getValue()) / 2.0;

                MeasurementValue measurementValue =
                        new MeasurementValue(newValue, newAccuracy, value.getMeasurement().getTimestamp());
                ExtendedMeasurement extendedMeasurement = new ExtendedMeasurement(value.getSensor(), value.getLocation(), measurementValue);
                // do not forward the first value (it only stands alone)
                out.collect(extendedMeasurement);
            }
            lastAverage.put(
                    sensor,
                    Tuple2.of(
                            value.getMeasurement().getAccuracy(),
                            value.getMeasurement().getValue()
                    ));
        }
    }

    /**
     * Converts SI units to locale-dependent units, i.e. °C to °F for the US. Adds a custom metric
     * to report temperatures in the US.
     */
    private static class ConvertToLocalTemperature extends
            RichMapFunction<ExtendedMeasurement, ExtendedMeasurement> {
        private static final long serialVersionUID = 1L;

        private transient MeanGauge normalizedTemperatureUS;

        @Override
        public void open(final Configuration parameters) {
            normalizedTemperatureUS = getRuntimeContext().getMetricGroup()
                    .gauge("normalizedTemperatureUSmean", new MeanGauge());
            getRuntimeContext().getMetricGroup().gauge(
                    "normalizedTemperatureUSmin", new MeanGauge.MinGauge(normalizedTemperatureUS));
            getRuntimeContext().getMetricGroup().gauge(
                    "normalizedTemperatureUSmax", new MeanGauge.MaxGauge(normalizedTemperatureUS));
        }

        @Override
        public ExtendedMeasurement map(ExtendedMeasurement value) {
            Location location = value.getLocation();
            if (GeoUtils.isInUS(location.getLongitude(), location.getLatitude())) {
                MeasurementValue measurement = value.getMeasurement();
                double normalized = WeatherUtils.celciusToFahrenheit(measurement.getValue());

                MeasurementValue localizedMeasurement =
                        new MeasurementValue(normalized, measurement.getAccuracy(), measurement.getTimestamp());
                normalizedTemperatureUS.addValue(normalized);
                return new ExtendedMeasurement(value.getSensor(), value.getLocation(), localizedMeasurement);
            }
            return value;
        }
    }

}
