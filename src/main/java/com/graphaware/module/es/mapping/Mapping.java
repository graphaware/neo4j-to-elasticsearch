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
import com.graphaware.common.representation.NodeRepresentation;
import com.graphaware.common.representation.PropertyContainerRepresentation;
import com.graphaware.common.representation.RelationshipRepresentation;
import com.graphaware.writer.thirdparty.*;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import org.neo4j.logging.Log;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.springframework.util.Assert.hasLength;
import static org.springframework.util.Assert.notNull;

public abstract class Mapping {
    private static final Log LOG = LoggerFactory.getLogger(Mapping.class);

    private final String keyProperty;
    private final String index;

    /**
     * @param index       ElasticSearch index name. Must not be <code>null</code> or empty.
     * @param keyProperty name of the node property that serves as the key, under which the node will be indexed in Elasticsearch. Must not be <code>null</code> or empty.
     * @param name        name of the mapping to use to convert Neo4j node/relationships to ElasticSearch documents.
     * @return mapping instance
     */
    public static Mapping getMapping(String index, String keyProperty, String name) {
        hasLength(index);
        hasLength(keyProperty);

        if (name.equals("default")) {
            return new DefaultMapping(index, keyProperty);
        }

        try {
            return (Mapping) Mapping.class.getClassLoader()
                    .loadClass(Mapping.class.getPackage().getName() + "." + name)
                    .getConstructor(String.class, String.class)
                    .newInstance(index, keyProperty);
        } catch (Exception e) {
            throw new RuntimeException("Could not instantiate Mapping \"" + name + "\"", e);
        }
    }

    /**
     *
     * @param index name/prefix of the ElasticSearch index that will store nodes and relationships for this mapping.
     * @param keyProperty name of the node property that serves as the key, under which the node will be indexed in Elasticsearch. Must not be <code>null</code> or empty.
     */
    public Mapping(String index, String keyProperty) {
        hasLength(keyProperty);
        notNull(keyProperty);

        this.index = index;
        this.keyProperty = keyProperty;
    }

    /**
     * Get the key under which the given {@link NodeRepresentation} or {@link RelationshipRepresentation} will be indexed in Elasticsearch.
     *
     * @param propertyContainer Node or relationship to be indexed.
     * @return key of the node.
     */
    protected final String getKey(PropertyContainerRepresentation propertyContainer) {
        return String.valueOf(propertyContainer.getProperties().get(keyProperty));
    }

    /**
     * @return name of the Elasticsearch index to use for indexing.
     */
    protected String getIndex() {
        return index;
    }

    /**
     * Convert a Neo4j representation to a ElasticSearch representation of a node.
     *
     * @param node A Neo4j node
     * @return a map of fields to store in ElasticSearch
     */
    protected Map<String, String> map(NodeRepresentation node) {
        Map<String, String> source = new LinkedHashMap<>();
        for (String key : node.getProperties().keySet()) {
              source.put(key, String.valueOf(node.getProperties().get(key)));
        }
        return source;
    }

    /**
     * Convert a Neo4j representation to a ElasticSearch representation of a relationship.
     *
     * @param relationship A Neo4j relationship
     * @return a map of fields to store in ElasticSearch
     */
    protected Map<String, String> map(RelationshipRepresentation relationship) {
        Map<String, String> source = new LinkedHashMap<>();
        for (String key : relationship.getProperties().keySet()) {
            source.put(key, String.valueOf(relationship.getProperties().get(key)));
        }
        return source;
    }

    /**
     * Create the ElasticSearch index(es) and initialize the mapping
     *
     * @param client The ElasticSearch client to use.
     * @param index Name/prefix of the ElasticSearch index to create and initialize
     * @throws Exception
     */
    public abstract void createIndexAndMapping(JestClient client, String index) throws Exception;

    public final List<BulkableAction<? extends JestResult>> getActions(WriteOperation operation) {
        switch (operation.getType()) {
            case NODE_CREATED:
                return createNode(((NodeCreated) operation).getDetails());

            case NODE_UPDATED:
                NodeUpdated nodeUpdated = (NodeUpdated) operation;
                return updateNode(nodeUpdated.getDetails().getPrevious(), nodeUpdated.getDetails().getCurrent());

            case NODE_DELETED:
                return deleteNode(((NodeDeleted) operation).getDetails());

            case RELATIONSHIP_CREATED:
                return createRelationship(((RelationshipCreated) operation).getDetails());

            case RELATIONSHIP_UPDATED:
                RelationshipUpdated relUpdated = (RelationshipUpdated) operation;
                return updateRelationship(relUpdated.getDetails().getPrevious(), relUpdated.getDetails().getCurrent());

            case RELATIONSHIP_DELETED:
                return deleteRelationship(((RelationshipDeleted) operation).getDetails());

            default:
                LOG.warn("Unsupported operation " + operation.getType());
                return Collections.emptyList();
        }
    }

    protected abstract List<BulkableAction<? extends JestResult>> createNode(NodeRepresentation node);

    protected abstract List<BulkableAction<? extends JestResult>> updateNode(NodeRepresentation before, NodeRepresentation after);

    protected abstract List<BulkableAction<? extends JestResult>> deleteNode(NodeRepresentation node);

    protected abstract List<BulkableAction<? extends JestResult>> createRelationship(RelationshipRepresentation relationship);

    protected abstract List<BulkableAction<? extends JestResult>> updateRelationship(RelationshipRepresentation before, RelationshipRepresentation after);

    protected abstract List<BulkableAction<? extends JestResult>> deleteRelationship(RelationshipRepresentation relationship);
}
