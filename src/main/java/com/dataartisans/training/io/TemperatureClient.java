package com.dataartisans.training.io;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class TemperatureClient {

    private static ExecutorService pool = Executors.newFixedThreadPool(30,
            new ThreadFactory() {
                private final ThreadFactory threadFactory = Executors.defaultThreadFactory();

                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = threadFactory.newThread(r);
                    thread.setName("temp-client-" + thread.getName());
                    return thread;
                }
            });

    private static final float  TEMPERATURE_LIMIT = 100;
    private        final Random rand              = new Random(42);

    public Float getTemperatureFor(String location) throws Exception {
        return new TemperatureSupplier().get();
    }

    public void asyncGetTemperatureFor(String location, Consumer<Float> callback) {

        CompletableFuture.supplyAsync(new TemperatureSupplier(), pool)
                         .thenAcceptAsync(callback, org.apache.flink.runtime.concurrent.Executors.directExecutor());
    }

    private class TemperatureSupplier implements Supplier<Float> {
        @Override
        public Float get() {
            try {
                Thread.sleep(rand.nextInt(5));
            } catch (InterruptedException e) {
                //Swallowing Interruption here
            }
            return rand.nextFloat() * TEMPERATURE_LIMIT;
        }
    }
}
