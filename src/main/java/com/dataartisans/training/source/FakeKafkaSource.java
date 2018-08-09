package com.dataartisans.training.source;

import com.dataartisans.training.entities.Measurement;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.List;
import java.util.Random;

public class FakeKafkaSource implements ParallelSourceFunction<byte[]> {

    private static final List<String> LOCATIONS = Lists.newArrayList("waldstadion", "boellenfalltor", "olympiastadion");

    private final Random       rand;
    private final ObjectMapper mapper;

    private volatile boolean cancelled;
    private final    int     maxOutOfOrderness;

    public FakeKafkaSource(final int maxOutOfOrderness) {
        this.maxOutOfOrderness = maxOutOfOrderness;
        this.rand = new Random();
        mapper = new ObjectMapper();
    }

    @Override
    public void run(final SourceContext<byte[]> sourceContext) throws Exception {

        //TODO: Replace this by source, which is reading the bytes initially to avoid the costly serialization
        //TODO: Make it stateful and deterministic by passing seed for Random
        //TODO: watermarking
        //TODO: Add malformed bytearrays
        while (!cancelled) {
            Measurement measurement = new Measurement(rand.nextInt(100),
                    rand.nextDouble() * 100, LOCATIONS.get(rand.nextInt(LOCATIONS.size())));
            byte[] serializedMeasurement = mapper.writeValueAsBytes(measurement);

            long nextEventTimeLag = System.currentTimeMillis() - rand.nextInt(maxOutOfOrderness);
            synchronized (sourceContext.getCheckpointLock()) {
                sourceContext.collectWithTimestamp(serializedMeasurement, nextEventTimeLag);
                // sourceContext.emitWatermark(new Watermark(nextEventTimeLag - maxOutOfOrderness));
            }
        }
    }

    @Override
    public void cancel() {
        cancelled = true;
    }


}
