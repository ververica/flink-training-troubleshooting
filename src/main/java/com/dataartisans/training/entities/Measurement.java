package com.dataartisans.training.entities;


import java.util.Objects;

public class Measurement {

    private int    sensorId;
    private double value;
    private String location;
    private float  temperature;

    public Measurement() {
    }

    public Measurement(final int sensorId, final double value, final String location) {
        this.sensorId = sensorId;
        this.value = value;
        this.location = location;
    }

    public Measurement(final int sensorId, final double value, final String location, final float temperature) {
        this.sensorId = sensorId;
        this.value = value;
        this.location = location;
        this.temperature = temperature;
    }

    public int getSensorId() {
        return sensorId;
    }

    public void setSensorId(final int sensorId) {
        this.sensorId = sensorId;
    }

    public double getValue() {
        return value;
    }

    public void setValue(final double value) {
        this.value = value;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(final String location) {
        this.location = location;
    }

    public float getTemperature() {
        return temperature;
    }

    public void setTemperature(final float temperature) {
        this.temperature = temperature;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Measurement that = (Measurement) o;
        return sensorId == that.sensorId &&
               Double.compare(that.value, value) == 0 &&
               Float.compare(that.temperature, temperature) == 0 &&
               Objects.equals(location, that.location);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sensorId, value, location, temperature);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Measurement{");
        sb.append("sensorId=").append(sensorId);
        sb.append(", value=").append(value);
        sb.append(", location='").append(location).append('\'');
        sb.append(", temperature=").append(temperature);
        sb.append('}');
        return sb.toString();
    }
}
