package com.dataartisans.training.source;

import com.dataartisans.training.entities.Measurement;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.io.*;
import java.util.List;
import java.util.Random;

public class DataGenerator {

    public static final int    NUM_OF_MEASUREMENTS = 100_000;
    public static final String MEASUREMENTS_PATH   = "src/main/resources/measurements.ser";

    public static void main(String[] args) throws IOException {

        Random rand = new Random(1);
        ObjectMapper mapper = new ObjectMapper();
        final List<String> locations = importLocations();

        List<byte[]> measurements = Lists.newArrayList();

        for (int i = 0; i < NUM_OF_MEASUREMENTS; i++) {
            Measurement nextMeasurement = new Measurement(rand.nextInt(100),
                    rand.nextDouble() * 100, locations.get(rand.nextInt(locations.size())));
            measurements.add(mapper.writeValueAsBytes(nextMeasurement));

            if (i % 1000 == 0) {
                System.out.println(i + "/" + NUM_OF_MEASUREMENTS);
            }
        }

        FileOutputStream fileOut =
                new FileOutputStream(MEASUREMENTS_PATH);
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(measurements);
        out.close();
        fileOut.close();
    }

    private static List<String> importLocations() throws IOException {
        List<String> locations = Lists.newArrayList();
        try (InputStream is = DataGenerator.class.getClass().getResourceAsStream("/cities.csv");
             BufferedReader br = new BufferedReader(new InputStreamReader(is));) {
            String city;
            while ((city = br.readLine()) != null) {
                locations.add(city);
            }
        }
        return locations;
    }

}
