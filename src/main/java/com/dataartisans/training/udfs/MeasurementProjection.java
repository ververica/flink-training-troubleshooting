package com.dataartisans.training.udfs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import org.apache.commons.collections.LRUMap;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.List;

public class MeasurementProjection implements MapFunction<JsonNode, JsonNode> {

    private List retainedFields;

    public MeasurementProjection(String... retainedFields) {
        new LRUMap().getMaximumSize();
        this.retainedFields = Lists.newArrayList(retainedFields);
    }

    @Override
    public JsonNode map(final JsonNode jsonNode) throws Exception {
        return ((ObjectNode) jsonNode).retain(retainedFields);
    }
}
