package com.dataartisans.training.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Measurement {

    private int    sensorId;
    private double value;
    private String location;
    private float  temperature;

    public Measurement(final int sensorId, final double value, final String location) {
        this.sensorId = sensorId;
        this.value = value;
        this.location = location;
    }
}
