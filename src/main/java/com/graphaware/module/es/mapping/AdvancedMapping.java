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

package com.graphaware.module.es.mapping;

import com.graphaware.common.log.LoggerFactory;
import com.graphaware.module.es.mapping.expression.NodeExpressions;
import com.graphaware.module.es.mapping.expression.RelationshipExpressions;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Index;
import io.searchbox.indices.mapping.PutMapping;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;

import java.util.*;

/**
 * This mapping indexes all documents in the same ElasticSearch index.
 *
 * Node's neo4j labels are stored in "_labels" field.
 * Relationships's neo4j type is stored in "_type" field.
 */
public class AdvancedMapping extends DefaultMapping {
    private static final Log LOG = LoggerFactory.getLogger(AdvancedMapping.class);

    public static final String NODE_TYPE = "node";
    public static final String RELATIONSHIP_TYPE = "relationship";
    public static final String LABELS_FIELD = "_labels";
    public static final String RELATIONSHIP_FIELD = "_relationship";

    private static final Map<String, Object> NODE_MAPPINGS = new HashMap<>();
    private static final Map<String, Object> RELATIONSHIP_MAPPINGS = new HashMap<>();

    static {
        Map<String, Object> rawField = new HashMap<>();
        rawField.put("type", "string");
        rawField.put("index", "not_analyzed");
        rawField.put("include_in_all", false);

        Map<String, Object> labelOrTypePropertyFields = new HashMap<>();
        labelOrTypePropertyFields.put("raw", rawField);

        Map<String, Object> labelOrTypeProperty = new HashMap<>();
        labelOrTypeProperty.put("type", "string");
        labelOrTypeProperty.put("fields", labelOrTypePropertyFields);

        Map<String
                , Object> nodeProperties = new HashMap<>();
        nodeProperties.put(LABELS_FIELD, labelOrTypeProperty);
        NODE_MAPPINGS.put("properties", nodeProperties);

        Map<String, Object> relationshipProperties = new HashMap<>();
        relationshipProperties.put(RELATIONSHIP_FIELD, labelOrTypeProperty);
        RELATIONSHIP_MAPPINGS.put("properties", relationshipProperties);
    }

    @Override
    protected List<BulkableAction<? extends JestResult>> createOrUpdateNode(NodeExpressions node) {
        Map<String, Object> source = map(node);
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();
        actions.add(new Index.Builder(source).index(getIndexFor(Node.class)).type(NODE_TYPE).id(getKey(node)).build());
        return actions;
    }

    @Override
    protected List<BulkableAction<? extends JestResult>> createOrUpdateRelationship(RelationshipExpressions r) {
        return Collections.singletonList(
                new Index.Builder(map(r)).index(getIndexFor(Relationship.class)).type(RELATIONSHIP_TYPE).id(getKey(r)).build()
        );
    }
     
    protected void addExtra(Map<String, Object> data, NodeExpressions node) {
        data.put(LABELS_FIELD, Arrays.asList(node.getLabels()));
    }
    
    protected void addExtra(Map<String, Object> data, RelationshipExpressions relationship) {
        data.put(RELATIONSHIP_FIELD, relationship.getType());
    }

    /**
     * Create non-analyzed fields for filtering:
     * - `_labels.raw` field for node labels
     * - `_relationship.raw` field for relationship types
     *
     * @param client The ElasticSearch client to use.
     *
     * @throws Exception
     */
    @Override
    public void createIndexAndMapping(JestClient client) throws Exception {
        super.createIndexAndMapping(client);

        // node mapping
        client.execute(new PutMapping.Builder(
                getIndexFor(Node.class), NODE_TYPE, NODE_MAPPINGS
        ).build());

        // relationship mapping
        client.execute(new PutMapping.Builder(
                getIndexFor(Relationship.class), RELATIONSHIP_TYPE, RELATIONSHIP_MAPPINGS
        ).build());
    }

    private static <T> Map.Entry<String, T> entry(String key, T value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    @SafeVarargs
    private static <T> Map<String, T> object(Map.Entry<String, T>... entries) {
        HashMap<String, T> map = new HashMap<>();
        for (Map.Entry<String, T> entry : entries) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }
}
