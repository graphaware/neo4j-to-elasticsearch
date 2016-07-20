package com.graphaware.module.es.mapping.json;

import com.graphaware.common.representation.NodeRepresentation;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.util.HashMap;
import java.util.Map;

public class Mapping {

    private String condition;

    private String type;

    private Map<String, String> properties;

    private SpelExpressionParser expressionParser;

    public String getCondition() {
        return condition;
    }

    public String getType() {
        return type;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public boolean supports(NodeRepresentation node) {
        if (null == condition) {
            return true;
        }
        Expression expression = getExpressionParser().parseExpression(condition);

        return (Boolean) expression.getValue(new NodeExpression(node));
    }

    public Action getCreateAction(NodeRepresentation node, Defaults defaults) {
        NodeExpression nodeExpression = new NodeExpression(node);
        String index = defaults.getIndex();
        String type = getType();
        String id = node.getProperties().get(defaults.getKeyProperty()).toString();
        Map<String, Object> source = new HashMap<>();

        for (String s : properties.keySet()) {
            Expression exp = getExpressionParser().parseExpression(properties.get(s));
            source.put(s, exp.getValue(nodeExpression));
        }

        return new Action(index, type, id, source);
    }

    private SpelExpressionParser getExpressionParser() {
        if (null == expressionParser) {
            expressionParser = new SpelExpressionParser();
        }

        return expressionParser;
    }
}
