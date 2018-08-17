package com.dataartisans.training.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FakeKafkaRecord {

    private long   timestamp;
    private byte[] key;
    private byte[] value;
    private int    partition;

}
