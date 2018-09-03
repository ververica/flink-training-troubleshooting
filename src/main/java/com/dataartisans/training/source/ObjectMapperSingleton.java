package com.dataartisans.training.source;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ObjectMapperSingleton {

    public static ObjectMapper getInstance() {
        return new ObjectMapper();
    }

}
