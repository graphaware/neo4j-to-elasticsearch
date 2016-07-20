package com.graphaware.module.es.mapping.json;

import java.util.HashMap;
import java.util.Map;

public class Action {

    private final String index;

    private final String type;

    private final String id;

    private final Map<String, Object> source;

    public Action(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.source = new HashMap<>();
    }

    public Action(String index, String type, String id, Map<String, Object> source) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.source = source;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public Map<String, Object> getSource() {
        return source;
    }
}
