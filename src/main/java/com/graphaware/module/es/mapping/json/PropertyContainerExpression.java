package com.graphaware.module.es.mapping.json;

import com.graphaware.common.representation.NodeRepresentation;
import com.graphaware.common.representation.PropertyContainerRepresentation;
import com.graphaware.common.representation.RelationshipRepresentation;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public abstract class PropertyContainerExpression {

    private static final String GRAPH_TYPE_NODE = "node";
    private static final String GRAPH_TYPE_RELATIONSHIP = "relationship";

    private static final String DEFAULT_TIMEZONE = "UTC";

    protected final PropertyContainerRepresentation propertyContainer;

    public PropertyContainerExpression(PropertyContainerRepresentation propertyContainer) {
        this.propertyContainer = propertyContainer;
    }

    public boolean hasProperty(String key) {
        return propertyContainer.getProperties().containsKey(key);
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
        return formatTime(propertyKey, format, DEFAULT_TIMEZONE);
    }

    public String formatTime(String propertyKey, String format, String timezone) {
        if (!propertyContainer.getProperties().containsKey(propertyKey)) {
            throw new IllegalArgumentException("Node doesn't contains the " + propertyKey + " property");
        }

        Long timestamp = Long.valueOf(propertyContainer.getProperties().get(propertyKey).toString());
        Date date = new Date(timestamp);
        DateFormat dateFormat = new SimpleDateFormat(format);
        dateFormat.setTimeZone(TimeZone.getTimeZone(timezone));

        return dateFormat.format(date);
    }

}
