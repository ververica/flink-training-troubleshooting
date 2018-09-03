package com.dataartisans.training.source;

import com.dataartisans.training.DoNotChangeThis;
import com.dataartisans.training.entities.Measurement;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

@DoNotChangeThis
public class SourceUtils {

    public static final Logger log = LoggerFactory.getLogger(SourceUtils.class);

    public static final int           NUM_OF_MEASUREMENTS = 100_000;
    public static final int           RANDOM_SEED         = 1;
    public static final float         FAILURE_RATE        = 0.0001f;
    public static final List<Integer> IDLE_PARTITIONS     = Arrays.asList(0, 4);

    public static FakeKafkaSource createFakeKafkaSource() {
        List<byte[]> serializedMeasurements = createSerializedMeasurements();
        return new FakeKafkaSource(RANDOM_SEED, FAILURE_RATE, IDLE_PARTITIONS, serializedMeasurements);
    }

    private static List<byte[]> createSerializedMeasurements() {
        Random rand = new Random(RANDOM_SEED);
        ObjectMapper mapper = new ObjectMapper();

        final List<String> locations = readLocationsFromFile();

        List<byte[]> measurements = new ArrayList<>();
        for (int i = 0; i < NUM_OF_MEASUREMENTS; i++) {
            Measurement nextMeasurement = new Measurement(rand.nextInt(100),
                    rand.nextDouble() * 100, locations.get(rand.nextInt(locations.size())));
            try {
                measurements.add(mapper.writeValueAsBytes(nextMeasurement));
            } catch (JsonProcessingException e) {
                log.error("Unable to serialize measurement.", e);
                throw new RuntimeException(e);
            }
        }
        return measurements;
    }

    private static List<String> readLocationsFromFile() {
        List<String> locations = new ArrayList<>();
        try (InputStream is = SourceUtils.class.getResourceAsStream("/cities.csv");
             BufferedReader br = new BufferedReader(new InputStreamReader(is));) {
            String city;
            while ((city = br.readLine()) != null) {
                locations.add(city);
            }
        } catch (IOException e) {
            log.error("Unable to read cities from file.", e);
            throw new RuntimeException(e);
        }
        return locations;
    }


}
