/*
 * Copyright (c) 2013-2016 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.graphaware.module.es.mapping.json;

import com.graphaware.common.log.LoggerFactory;
import com.graphaware.common.representation.NodeRepresentation;
import com.graphaware.common.representation.PropertyContainerRepresentation;
import com.graphaware.common.representation.RelationshipRepresentation;
import org.neo4j.logging.Log;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.util.HashMap;
import java.util.Map;

public class GraphDocumentMapper {

    private static final Log LOG = LoggerFactory.getLogger(GraphDocumentMapper.class);

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

    public DocumentRepresentation getDocumentRepresentation(NodeRepresentation node, DocumentMappingDefaults defaults) {
        return getDocumentRepresentation(node, defaults, true);
    }

    public DocumentRepresentation getDocumentRepresentation(NodeRepresentation node, DocumentMappingDefaults defaults, boolean buildSource) {
        NodeExpression nodeExpression = new NodeExpression(node);
        Map<String, Object> source = new HashMap<>();
        String i = getIndex(nodeExpression, defaults.getDefaultNodesIndex());
        String id = node.getProperties().get(defaults.getKeyProperty()).toString();

        if (buildSource) {
            if (null != properties) {
                for (String s : properties.keySet()) {
                    Expression exp = getExpressionParser().parseExpression(properties.get(s));
                    source.put(s, exp.getValue(nodeExpression));
                }
            }

            if (defaults.includeRemainingProperties()) {
                for (String s : node.getProperties().keySet()) {
                    if (!defaults.getBlacklistedNodeProperties().contains(s)) {
                        source.put(s, node.getProperties().get(s));
                    }
                }
            }
        }
        return new DocumentRepresentation(i, getType(nodeExpression), id, source);
    }

    protected String getType(PropertyContainerExpression expression) {
        String t = type;
        if (t.contains("(") && t.contains(")")) {
            Expression typeExpression = getExpressionParser().parseExpression(t);
            t = typeExpression.getValue(expression).toString();
        }

        if (t == null || t.equals("")) {
            LOG.error("Unable to build type name");
            throw new RuntimeException("Unable to build type name");
        }

        return t;
    }

    protected String getIndex(PropertyContainerExpression expression, String defaultIndex)
    {
        String i = index != null ? index : defaultIndex;
        if (i.contains("(") && i.contains(")")) {
            Expression indexExpression = getExpressionParser().parseExpression(i);
            i = indexExpression.getValue(expression).toString();
        }

        if (i == null || i.equals("")) {
            LOG.error("Unable to build index name");
            throw new RuntimeException("Unable to build index name");
        }

        return i;
    }

    public DocumentRepresentation getDocumentRepresentation(RelationshipRepresentation relationship, DocumentMappingDefaults defaults) {
        return getDocumentRepresentation(relationship, defaults, true);
    }

    public DocumentRepresentation getDocumentRepresentation(RelationshipRepresentation relationship, DocumentMappingDefaults defaults, boolean buildSource) {
        RelationshipExpression relationshipExpression = new RelationshipExpression(relationship);
        Map<String, Object> source = new HashMap<>();

        if (buildSource) {
            if (null != properties) {
                for (String s : properties.keySet()) {
                    Expression exp = getExpressionParser().parseExpression(properties.get(s));
                    source.put(s, exp.getValue(relationshipExpression));
                }
            }

            if (defaults.includeRemainingProperties()) {
                for (String s : relationship.getProperties().keySet()) {
                    if (!defaults.getBlacklistedRelationshipProperties().contains(s)) {
                        source.put(s, relationship.getProperties().get(s));
                    }
                }
            }
        }
        String i = getIndex(relationshipExpression, defaults.getDefaultRelationshipsIndex());
        String id = relationship.getProperties().get(defaults.getKeyProperty()).toString();
        return new DocumentRepresentation(i, getType(relationshipExpression), id, source);

    }

    private SpelExpressionParser getExpressionParser() {
        if (null == expressionParser) {
            expressionParser = new SpelExpressionParser();
        }

        return expressionParser;
    }        
}
