package com.graphaware.module.es.mapping.json;

import com.graphaware.common.representation.NodeRepresentation;

public class NodeExpression {

    private final NodeRepresentation node;

    public NodeExpression(NodeRepresentation node) {
        this.node = node;
    }

    public boolean hasLabel(String label) {
        for (String s : node.getLabels()) {
            if (s.equals(label)) {
                return true;
            }
        }

        return false;
    }

    public Object getProperty(String key) {
        for (String s : node.getProperties().keySet()) {
            if (s.equals(key)) {
                return node.getProperties().get(s);
            }
        }

        return null;
    }
}
