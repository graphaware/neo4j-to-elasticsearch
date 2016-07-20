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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.graphaware.common.log.LoggerFactory;
import com.graphaware.common.representation.NodeRepresentation;
import com.graphaware.common.representation.PropertyContainerRepresentation;
import com.graphaware.common.representation.RelationshipRepresentation;
import com.graphaware.writer.thirdparty.*;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;

import static org.springframework.util.Assert.hasLength;
import static org.springframework.util.Assert.notNull;

public abstract class Mapping implements MappingDefinition {
    private static final Log LOG = LoggerFactory.getLogger(Mapping.class);

    protected String keyProperty;
    protected String index;

    protected final ObjectMapper mapper = new ObjectMapper();

    /**
     *
     * @param index name/prefix of the ElasticSearch index that will store nodes and relationships for this mapping.
     * @param keyProperty name of the node property that serves as the key, under which the node will be indexed in Elasticsearch. Must not be <code>null</code> or empty.
     */
    public Mapping(String index, String keyProperty) {
        hasLength(keyProperty);
        notNull(keyProperty);

        //this.index = index;
        //this.keyProperty = keyProperty;
    }

    public Mapping() {
    }

    /**
     * Get the key under which the given {@link NodeRepresentation} or {@link RelationshipRepresentation} will be indexed in Elasticsearch.
     *
     * @param propertyContainer Node or relationship to be indexed.
     * @return key of the node.
     */
    protected final String getKey(PropertyContainerRepresentation propertyContainer) {
        return String.valueOf(propertyContainer.getProperties().get(getKeyProperty()));
    }

    /**
     * @return name of the ElasticSearch index name prefix to use for indexing.
     */
    protected String getIndexPrefix() {
        return index;
    }

    /**
     * Convert a Neo4j representation to a ElasticSearch representation of a node.
     *
     * @param node A Neo4j node
     * @return a map of fields to store in ElasticSearch
     */
    protected Map<String, String> map(NodeRepresentation node) {
        Map<String, String> source = new HashMap<>();
        for (String key : node.getProperties().keySet()) {
              source.put(key, String.valueOf(node.getProperties().get(key)));
        }
        return source;
    }
    
    
    
    protected String getJson(NodeRepresentation node) {
        Map<String,Object> data = new HashMap<>();
        
        for (String key : node.getProperties().keySet()) {
              data.put(key, node.getProperties().get(key));
        }
        addExtra(data, node);
        try {
            String json = mapper.writeValueAsString(data);
            return json;
        } catch (JsonProcessingException ex) {
            LOG.error("Error while creating json from node: " + node.toString(), ex);
            throw new RuntimeException("Error while creating json from node: " + node.toString(), ex);
        }
    }
    
    protected String getJson(RelationshipRepresentation relationship) {
        Map<String,Object> data = new HashMap<>();
        
        for (String key : relationship.getProperties().keySet()) {
              data.put(key, relationship.getProperties().get(key));
        }
        addExtra(data, relationship);
        try {
            return mapper.writeValueAsString(data);
        } catch (JsonProcessingException ex) {
            LOG.error("Error while creating json from node: " + relationship.toString(), ex);
            throw new RuntimeException("Error while creating json from node: " + relationship.toString(), ex);
        }
    }
    
    protected void addExtra(Map<String, Object> data, NodeRepresentation node) {
        
    }
    
    protected void addExtra(Map<String, Object> data, RelationshipRepresentation relationship) {
        
    }

    /**
     * Convert a Neo4j representation to a ElasticSearch representation of a relationship.
     *
     * @param relationship A Neo4j relationship
     * @return a map of fields to store in ElasticSearch
     */
    protected Map<String, String> map(RelationshipRepresentation relationship) {
        Map<String, String> source = new HashMap<>();
        for (String key : relationship.getProperties().keySet()) {
            source.put(key, String.valueOf(relationship.getProperties().get(key)));
        }
        return source;
    }

    /**
     * Create the ElasticSearch index(es) and initialize the mapping
     *
     * @param client The ElasticSearch client to use.
     * @throws Exception
     */
    public void createIndexAndMapping(JestClient client) throws Exception {
        List<String> indexes = Arrays.asList(
                getIndexFor(Node.class),
                getIndexFor(Relationship.class)
        );
        indexes.stream().distinct().forEach(index -> {
            try {
                createIndexAndMapping(client, index);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void createIndexAndMapping(JestClient client, String index) throws Exception {
        if (client.execute(new IndicesExists.Builder(index).build()).isSucceeded()) {
            LOG.info("Index " + index + " already exists in ElasticSearch.");
            return;
        }

        LOG.info("Index " + index + " does not exist in ElasticSearch, creating...");

        final JestResult execute = client.execute(new CreateIndex.Builder(index).build());

        if (execute.isSucceeded()) {
            LOG.info("Created ElasticSearch index.");
        } else {
            LOG.error("Failed to create ElasticSearch index. Details: " + execute.getErrorMessage());
        }
    }

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

    public abstract <T extends PropertyContainer> String getIndexFor(Class<T> searchedType);
}
