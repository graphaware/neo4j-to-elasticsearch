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

import com.graphaware.common.representation.NodeRepresentation;
import com.graphaware.writer.thirdparty.*;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.springframework.util.Assert.hasLength;

public abstract class Mapping {
    private static final Logger LOG = LoggerFactory.getLogger(Mapping.class);

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
     * @param keyProperty name of the node property that serves as the key, under which the node will be indexed in Elasticsearch. Must not be <code>null</code> or empty.
     */
    public Mapping(String index, String keyProperty) {
        hasLength(keyProperty);

        this.index = index;
        this.keyProperty = keyProperty;
    }

    /**
     * Get the key under which the given {@link NodeRepresentation} will be indexed in Elasticsearch.
     *
     * @param node to be indexed.
     * @return key of the node.
     */
    protected final String getKey(NodeRepresentation node) {
        return String.valueOf(node.getProperties().get(keyProperty));
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
    protected abstract Map<String, String> map(NodeRepresentation node);

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
                return createNode((NodeCreated) operation);

            case NODE_UPDATED:
                return updateNode((NodeUpdated) operation);

            case NODE_DELETED:
                return deleteNode((NodeDeleted) operation);

            default:
                LOG.warn("Unsupported operation " + operation.getType());
                return Collections.emptyList();
        }
    }

    protected abstract List<BulkableAction<? extends JestResult>> createNode(NodeCreated operation);

    protected abstract List<BulkableAction<? extends JestResult>> updateNode(NodeUpdated operation);

    protected abstract List<BulkableAction<? extends JestResult>> deleteNode(NodeDeleted operation);

}
