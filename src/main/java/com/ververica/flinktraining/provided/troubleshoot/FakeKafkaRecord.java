package com.ververica.flinktraining.provided.troubleshoot;

import com.ververica.flinktraining.provided.DoNotChangeThis;

import java.util.Arrays;
import java.util.Objects;

@DoNotChangeThis
public class FakeKafkaRecord {

    private long   timestamp;
    private byte[] key;
    private byte[] value;
    private int    partition;

    public FakeKafkaRecord() {
    }

    public FakeKafkaRecord(final long timestamp, final byte[] key, final byte[] value, final int partition) {
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.partition = partition;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(final byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(final byte[] value) {
        this.value = value;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(final int partition) {
        this.partition = partition;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final FakeKafkaRecord that = (FakeKafkaRecord) o;
        return timestamp == that.timestamp &&
               partition == that.partition &&
               Arrays.equals(key, that.key) &&
               Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(timestamp, partition);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FakeKafkaRecord{");
        sb.append("timestamp=").append(timestamp);
        sb.append(", key=").append(Arrays.toString(key));
        sb.append(", value=").append(Arrays.toString(value));
        sb.append(", partition=").append(partition);
        sb.append('}');
        return sb.toString();
    }
}
