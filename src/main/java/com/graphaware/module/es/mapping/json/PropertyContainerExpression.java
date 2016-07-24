package com.graphaware.module.es.mapping.json;

import com.graphaware.common.representation.NodeRepresentation;
import com.graphaware.common.representation.PropertyContainerRepresentation;
import com.graphaware.common.representation.RelationshipRepresentation;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public abstract class PropertyContainerExpression {

    private static final String GRAPH_TYPE_NODE = "node";
    private static final String GRAPH_TYPE_RELATIONSHIP = "relationship";

    protected final PropertyContainerRepresentation propertyContainer;

    public PropertyContainerExpression(PropertyContainerRepresentation propertyContainer) {
        this.propertyContainer = propertyContainer;
    }

    public Object getProperty(String key, Object def) {
        return propertyContainer.getProperties().getOrDefault(key, def);
    }

    public Object getProperty(String key) {
        for (Object s : propertyContainer.getProperties().keySet()) {
            if (s.equals(key)) {
                return propertyContainer.getProperties().get(s);
            }
        }

        return null;
    }

    public String getGraphType() {
        if (propertyContainer instanceof NodeRepresentation) {
            return GRAPH_TYPE_NODE;
        }

        if (propertyContainer instanceof RelationshipRepresentation) {
            return GRAPH_TYPE_RELATIONSHIP;
        }

        throw new RuntimeException("Property Container is not valid");
    }

    public String formatTime(String propertyKey, String format) {
        if (!propertyContainer.getProperties().containsKey(propertyKey)) {
            throw new IllegalArgumentException("Node doesn't contains the " + propertyKey + " property");
        }

        Long timestamp = Long.valueOf(propertyContainer.getProperties().get(propertyKey).toString());
        Date date = new Date(timestamp);
        DateFormat dateFormat = new SimpleDateFormat(format);

        return dateFormat.format(date);
    }

}
