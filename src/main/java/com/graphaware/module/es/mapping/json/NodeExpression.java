package com.graphaware.module.es.mapping.json;

import com.graphaware.common.representation.NodeRepresentation;

public class NodeExpression extends PropertyContainerExpression {

    public NodeExpression(NodeRepresentation nodeRepresentation) {
        super(nodeRepresentation);
    }

    public boolean hasLabel(String label) {
        for (String s : ((NodeRepresentation) propertyContainer).getLabels()) {
            if (s.equals(label)) {
                return true;
            }
        }

        return false;
    }

    public String[] getLabels() {
        return ((NodeRepresentation) propertyContainer).getLabels();
    }

    public boolean allNodes() {
        return true;
    }

}
