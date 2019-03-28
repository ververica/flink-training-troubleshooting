package com.ververica.training.entities;


import java.util.Objects;

public class SimpleMeasurement {

    private int    sensorId;
    private double value;
    private String location;

    public SimpleMeasurement() {
    }

    public SimpleMeasurement(final int sensorId, final double value, final String location, final String measurementInformation) {
        this.sensorId = sensorId;
        this.value = value;
        this.location = location;
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

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SimpleMeasurement that = (SimpleMeasurement) o;
        return sensorId == that.sensorId &&
               Double.compare(that.value, value) == 0 &&
               Objects.equals(location, that.location);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sensorId, value, location);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Measurement{");
        sb.append("sensorId=").append(sensorId);
        sb.append(", value=").append(value);
        sb.append(", location='").append(location).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
