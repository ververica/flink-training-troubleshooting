package com.dataartisans.training.udfs;

import org.apache.flink.api.common.functions.MapFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.collections.map.LRUMap;

import java.util.Arrays;
import java.util.List;

public class MeasurementProjection implements MapFunction<JsonNode, JsonNode> {

    private List retainedFields;

    public MeasurementProjection(String... retainedFields) {
        new LRUMap().maxSize();
        this.retainedFields = Arrays.asList(retainedFields);
    }

    @Override
    public JsonNode map(final JsonNode jsonNode) throws Exception {
        return ((ObjectNode) jsonNode).retain(retainedFields);
    }
}
