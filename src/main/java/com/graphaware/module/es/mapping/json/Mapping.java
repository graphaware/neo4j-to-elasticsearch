package com.graphaware.module.es.mapping.json;

import com.graphaware.common.representation.NodeRepresentation;
import com.graphaware.common.representation.PropertyContainerRepresentation;
import com.graphaware.common.representation.RelationshipRepresentation;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.util.HashMap;
import java.util.Map;

public class Mapping {

    private String condition;

    private String index;

    private String type;

    private Map<String, String> properties;

    private SpelExpressionParser expressionParser;

    public String getCondition() {
        return condition;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public boolean supports(PropertyContainerRepresentation element) {
        if (null == condition) {
            return true;
        }
        Expression expression = getExpressionParser().parseExpression(condition);

        if (element instanceof NodeRepresentation) {
            return (Boolean) expression.getValue(new NodeExpression((NodeRepresentation) element));
        } else if (element instanceof RelationshipRepresentation) {
            return (Boolean) expression.getValue(new RelationshipExpression((RelationshipRepresentation) element));
        }

        throw new RuntimeException("Element is nor Node nor Relationship");
    }

    public Action getCreateAction(NodeRepresentation node, Defaults defaults) {
        NodeExpression nodeExpression = new NodeExpression(node);
        String i = index != null ? index : defaults.getDefaultNodesIndex();
        String type = getType();
        String id = node.getProperties().get(defaults.getKeyProperty()).toString();
        Map<String, Object> source = new HashMap<>();

        if (null != properties) {
            for (String s : properties.keySet()) {
                Expression exp = getExpressionParser().parseExpression(properties.get(s));
                source.put(s, exp.getValue(nodeExpression));
            }
        }

        if (defaults.includeRemainingProperties()) {
            for (String s : node.getProperties().keySet()) {
                source.put(s, node.getProperties().get(s));
            }
        }

        return new Action(i, type, id, source);
    }

    public Action getCreateAction(RelationshipRepresentation relationship, Defaults defaults) {
        RelationshipExpression relationshipExpression = new RelationshipExpression(relationship);
        String i = index != null ? index : defaults.getDefaultRelationshipsIndex();
        String type = getType();
        String id = relationship.getProperties().get(defaults.getKeyProperty()).toString();
        Map<String, Object> source = new HashMap<>();

        if (null != properties) {
            for (String s : properties.keySet()) {
                Expression exp = getExpressionParser().parseExpression(properties.get(s));
                source.put(s, exp.getValue(relationshipExpression));
            }
        }

        if (defaults.includeRemainingProperties()) {
            for (String s : relationship.getProperties().keySet()) {
                source.put(s, relationship.getProperties().get(s));
            }
        }

        return new Action(i, type, id, source);

    }

    private SpelExpressionParser getExpressionParser() {
        if (null == expressionParser) {
            expressionParser = new SpelExpressionParser();
        }

        return expressionParser;
    }
}
