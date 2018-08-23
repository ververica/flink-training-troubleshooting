package com.dataartisans.training;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

public class FakeKafkaSource implements ParallelSourceFunction<byte[]> {

    private final Random rand;


    private volatile boolean cancelled;

    public FakeKafkaSource() {
        this.rand = new Random();
    }

    @Override
    public void run(final SourceContext<byte[]> sourceContext) throws Exception {
        while(!cancelled) {
            byte[] next = new byte[100];
            rand.nextBytes(next);
            synchronized (sourceContext.getCheckpointLock()) {
                sourceContext.collect(next);
                Thread.sleep(100);
            }
        }
    }
    @Override
    public void cancel() {
        cancelled = true;
    }
}
