package com.dataartisans.training.io;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class TemperatureClient {

    private ExecutorService pool = Executors.newFixedThreadPool(30,
            new ThreadFactoryBuilder().setNameFormat("temp-client-thread-%d").build());

    private static final float  TEMPERATURE_LIMIT = 100;
    private              Random rand              = new Random(42);

    public Float getTemperatureFor(String location) throws Exception {
        return new TemperatureSupplier().get();
    }

    public void asyncGetTemperatureFor(String location, Consumer<Float> callback) {

        CompletableFuture.supplyAsync(new TemperatureSupplier(), pool)
                         .thenAcceptAsync(callback, org.apache.flink.runtime.concurrent.Executors.directExecutor());
    }

    private class TemperatureSupplier implements Supplier<Float> {
        //TODO return Optional.empty() for some keys
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
