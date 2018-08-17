package com.dataartisans.training.io;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TemperatureClient {

    private ExecutorService pool = Executors.newFixedThreadPool(30);

    private static final float  TEMPERATURE_LIMIT = 100;
    private              Random rand              = new Random(42);

    public Optional<Float> getTemperatureFor(String location) throws Exception {
        return new TemperatureQuery().call();
    }

    public Future<Optional<Float>> asyncGetTemperatureFor(String location) {
        return pool.submit(new TemperatureQuery());

    }

    private class TemperatureQuery implements Callable<Optional<Float>> {
        //TODO return Optional.empty() for some keys
        @Override
        public Optional<Float> call() throws Exception {
            Thread.sleep(rand.nextInt(10));
            return Optional.of(rand.nextFloat() * TEMPERATURE_LIMIT);
        }
    }
}
