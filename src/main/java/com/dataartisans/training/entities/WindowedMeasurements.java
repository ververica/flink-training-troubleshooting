package com.dataartisans.training.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WindowedMeasurements {

    private long   windowStart;
    private long   windowEnd;
    private String location;
    private long   eventsPerWindow;
    private double sumPerWindow;

    public double getAvgTemperature() {
        return sumPerWindow / eventsPerWindow;
    }

}
