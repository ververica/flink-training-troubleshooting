package com.dataartisans.training.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Measurement {

    //TODO add much more fields :)

    private int    sensorId;
    private double value;
    private String location;

}
