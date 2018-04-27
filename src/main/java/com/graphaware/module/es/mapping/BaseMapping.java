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

import com.graphaware.common.expression.EntityExpressions;
import com.graphaware.common.log.LoggerFactory;
import com.graphaware.common.representation.DetachedEntity;
import com.graphaware.module.es.mapping.expression.NodeExpressions;
import com.graphaware.module.es.mapping.expression.RelationshipExpressions;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;

import java.lang.reflect.Array;
import java.util.*;

public abstract class BaseMapping implements Mapping {

    public static final String NATIVE_ID = "ID()";

    private static final Log LOG = LoggerFactory.getLogger(BaseMapping.class);

    private static final String DEFAULT_INDEX = "neo4j-index";
    private static final String DEFAULT_KEY_PROPERTY = "uuid";
    private static final String DEFAULT_FORCE_STRINGS = "false";

    protected String keyProperty;
    protected String indexPrefix;
    protected boolean forceStrings;
    protected GraphDatabaseService database;

    public BaseMapping() {
    }

    @Override
    public void configure(Map<String, String> config) {
        indexPrefix = config.getOrDefault("index", DEFAULT_INDEX).trim();
        if (indexPrefix.isEmpty()) { indexPrefix = DEFAULT_INDEX; }
        LOG.info("ElasticSearch index-prefix set to %s", indexPrefix);

        keyProperty = config.getOrDefault("keyProperty", DEFAULT_KEY_PROPERTY).trim();
        if (keyProperty.isEmpty()) { keyProperty = DEFAULT_KEY_PROPERTY; }
        LOG.info("ElasticSearch key-property set to %s", keyProperty);

        forceStrings = config.getOrDefault("forceStrings", DEFAULT_FORCE_STRINGS).trim().toLowerCase().equals("true");
        LOG.info("ElasticSearch force-strings set to %s", forceStrings);
    }

    /**
     * @return the name of the ElasticSearch index name prefix to use for indexing.
     */
    protected String getIndexPrefix() {
        return indexPrefix;
    }

    /**+
     * @return the node/relationship property used as document ID in ElasticSearch (usually "uuid")
     */
    @Override
    public String getKeyProperty() {
        return keyProperty;
    }

    /**
     * Get the key under which the given {@link NodeExpressions} or {@link RelationshipExpressions} will be indexed in Elasticsearch.
     *
     * @param entity Node or relationship to be indexed.
     * @return key of the node.
     */
    protected final String getKey(DetachedEntity entity) {
        if (getKeyProperty().equals(NATIVE_ID)) {
            return String.valueOf(entity.getGraphId());
        } else {
            return String.valueOf(entity.getProperties().get(getKeyProperty()));
        }
    }

    /**
     * Creates an ElasticSearch document from a node or relationship.
     * Includes all properties, except the keyProperty (usually "uuid") which is already the ElasticSearch document ID.
     *
     * @param item The node or relationship to build the ElasticSearch representation for.
     * @return a document to index in ElasticSearch
     */
    private Map<String, Object> commonMap(EntityExpressions item) {
        String keyProperty = getKeyProperty();
        Map<String, Object> source = new HashMap<>();
        Map<String, Object> properties = item.getProperties();
        if (item.getProperties() != null) {
            for (String key : properties.keySet()) {
                if (keyProperty.equals(key)) {
                    // don't index the key property in ElasticSearch
                    continue;
                }
                Object value = properties.get(key);
                if (value.getClass().isArray()) {
                    int length = Array.getLength(value);
                    Object[] newValues = new Object[length];
                    for (int i = 0; i < length; i ++) {
                        newValues[i] = normalizeProperty(Array.get(value, i));
                    }
                    value = newValues;
                } else {
                    value = normalizeProperty(value);
                }
                source.put(key, value);
            }
        }
        return source;
    }

    /**
     * Can be used to convert all properties to their String representation.
     *
     * @param propertyValue Property value to normalize
     * @return normalized property value
     */
    protected Object normalizeProperty(Object propertyValue) {
        return forceStrings ? String.valueOf(propertyValue) : propertyValue;
    }

    /**
     * Convert a Neo4j representation to a ElasticSearch representation of a node.
     *
     * @param node A Neo4j node
     * @return a map of fields to store in ElasticSearch
     */
    protected Map<String, Object> map(NodeExpressions node) {
        Map<String, Object> source = commonMap(node);
        addExtra(source, node);
        return source;
    }

    /**
     * Convert a Neo4j representation to a ElasticSearch representation of a relationship.
     *
     * @param relationship A Neo4j relationship
     * @return a map of fields to store in ElasticSearch
     */
    protected Map<String, Object> map(RelationshipExpressions relationship) {
        Map<String, Object> source = commonMap(relationship);
        addExtra(source, relationship);
        return source;
    }

    protected void addExtra(Map<String, Object> data, NodeExpressions node) { }

    protected void addExtra(Map<String, Object> data, RelationshipExpressions relationship) { }

    /**
     * Create the ElasticSearch index(es) and initialize the mapping
     *
     * @param client The ElasticSearch client to use.
     *
     * @throws Exception
     */
    public void createIndexAndMapping(JestClient client) throws Exception {
        List<Class> indexes = Arrays.asList(
                Node.class,
                Relationship.class
        );
        indexes.stream().distinct().forEach(indexType -> {
            try {
                createIndexAndMapping(client, indexType);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * @param client an ElasticSearch client
     * @param indexType the name of the index to create
     * @return true when an index wa created (false if is already existed or failed to create)
     * @throws Exception
     */
    protected <T extends Entity> boolean createIndexAndMapping(JestClient client, Class<T> indexType) throws Exception {
        String indexName = getIndexFor(indexType);

        if (client.execute(new IndicesExists.Builder(indexName).build()).isSucceeded()) {
            LOG.info("Index " + indexName + " already exists in ElasticSearch.");
            return false;
        }

        LOG.info("Index " + indexName + " does not exist in ElasticSearch, creating...");

        final JestResult execute = client.execute(new CreateIndex.Builder(indexName).build());

        if (execute.isSucceeded()) {
            LOG.info("Created ElasticSearch index.");
            return true;
        } else {
            LOG.error("Failed to create ElasticSearch index. Details: " + execute.getErrorMessage());
            return false;
        }
    }

    public abstract <T extends Entity> String getIndexFor(Class<T> searchedType);

    protected List<BulkableAction<? extends JestResult>> emptyActions() {
        return new ArrayList<>();
    }

    @Override
    public boolean bypassInclusionPolicies() {
        return false;
    }

    @Override
    public void setDatabase(GraphDatabaseService database) {
        this.database = database;
    }

    protected GraphDatabaseService getDatabase() {
        return database;
    }
}
