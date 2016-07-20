package com.graphaware.module.es.mapping.json;

import com.graphaware.common.representation.NodeRepresentation;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.ClassUtils;

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

    public boolean supports(NodeRepresentation node) {
        if (null == condition) {
            return true;
        }
        Expression expression = getExpressionParser().parseExpression(condition);

        return (Boolean) expression.getValue(new NodeExpression(node));
    }

    public Action getCreateAction(NodeRepresentation node, Defaults defaults) {
        NodeExpression nodeExpression = new NodeExpression(node);
        String i = index != null ? index : defaults.getIndex();
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
                Object o = node.getProperties().get(s);
                if (ClassUtils.isPrimitiveOrWrapper(o.getClass()) || o instanceof String) {
                    source.put(s, node.getProperties().get(s));
                }
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
