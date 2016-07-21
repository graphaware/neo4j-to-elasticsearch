package com.graphaware.module.es.mapping.json;

import com.graphaware.common.representation.RelationshipRepresentation;

public class RelationshipExpression extends PropertyContainerExpression {

    public RelationshipExpression(RelationshipRepresentation relationship) {
        super(relationship);
    }

    public boolean hasType(String type) {
        return getRelationship().getType().equals(type);
    }

    private RelationshipRepresentation getRelationship() {
        return (RelationshipRepresentation) propertyContainer;
    }

}
