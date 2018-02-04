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
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.indices.mapping.PutMapping;
import org.neo4j.graphdb.Entity;
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
    public List<BulkableAction<? extends JestResult>> deleteNode(NodeExpressions node) {
        return Collections.singletonList(
                new Delete.Builder(getKey(node))
                        .index(getIndexFor(Node.class))
                        .type(NODE_TYPE)
                        .build()
        );
    }

    @Override
    public List<BulkableAction<? extends JestResult>> deleteRelationship(RelationshipExpressions r) {
        return Collections.singletonList(
                new Delete.Builder(getKey(r))
                        .index(getIndexFor(Relationship.class))
                        .type(RELATIONSHIP_TYPE)
                        .build()
        );
    }

    @Override
    protected List<BulkableAction<? extends JestResult>> createOrUpdateNode(NodeExpressions node) {
        return Collections.singletonList(
                new Index.Builder(map(node))
                        .index(getIndexFor(Node.class))
                        .type(NODE_TYPE)
                        .id(getKey(node))
                        .build()
        );
    }

    @Override
    protected List<BulkableAction<? extends JestResult>> createOrUpdateRelationship(RelationshipExpressions r) {
        return Collections.singletonList(
                new Index.Builder(map(r))
                        .index(getIndexFor(Relationship.class))
                        .type(RELATIONSHIP_TYPE)
                        .id(getKey(r))
                        .build()
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
     * @param client an ElasticSearch client
     * @param indexType the name of the index to create
     * @param <T> Node.class or Relationship.class
     * @return true in case of success
     * @throws Exception
     */
    @Override
    protected <T extends Entity> boolean createIndexAndMapping(JestClient client, Class<T> indexType) throws Exception {
        boolean created = super.createIndexAndMapping(client, indexType);
        if (!created) {
            return false;
        }

        Map<String, Object> mappings;
        String esType;

        if (indexType.equals(Node.class)) {
            mappings = NODE_MAPPINGS;
            esType = NODE_TYPE;
        } else {
            mappings = RELATIONSHIP_MAPPINGS;
            esType = RELATIONSHIP_TYPE;
        }

        JestResult e = client.execute(
                new PutMapping.Builder(getIndexFor(indexType), esType, mappings).build()
        );

        if (!e.isSucceeded()) {
            LOG.warn("Mapping creation error: " + e.getErrorMessage());
        }

        return true;
    }

}
