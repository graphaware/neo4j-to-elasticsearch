/*
 * Copyright (c) 2015 GraphAware
 *
 * This file is part of GraphAware.
 *
 * GraphAware is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.es;

import com.graphaware.common.representation.NodeRepresentation;
import com.graphaware.writer.thirdparty.*;
import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * A {@link ThirdPartyWriter} to Elasticsearch (https://www.elastic.co/).
 */
public class ElasticSearchBulkWriter extends ElasticSearchWriter {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchBulkWriter.class);


    public ElasticSearchBulkWriter(String uri, String port, String keyProperty, String index, boolean retryOnError) {
        super(uri, port, keyProperty, index, retryOnError);
        LOG.warn("Starting bulk writer");
    }

    public ElasticSearchBulkWriter(int queueCapacity, String uri, String port, String keyProperty, String index, boolean retryOnError) {
        super(uri, port, keyProperty, index, retryOnError);
        LOG.warn("Starting bulk writer");
    }

    @Override
    protected void processOperations(List<Collection<WriteOperation<?>>> list) {
        List<Collection<WriteOperation<?>>> allFailed = new LinkedList<>();
        
        Bulk.Builder bulkBuilder = startBulkOperation();
        
        for (Collection<WriteOperation<?>> collection : list) {
            for (WriteOperation<?> operation : collection) {
                switch (operation.getType()) {
                    case NODE_CREATED:
                        createNode((NodeCreated) operation, bulkBuilder);; 
                        break;
                    case NODE_UPDATED:
                        updateNode((NodeUpdated) operation, bulkBuilder);
                        break;
                    case NODE_DELETED:
                        deleteNode((NodeDeleted) operation, bulkBuilder);
                        break;
                    default:
                        LOG.warn("Unsupported operation " + operation.getType());
                }
            }
        }
        executeBulk(bulkBuilder);
        
        if (getRetryOnError() && !executeBulk(bulkBuilder)) {
            retry(list);
        }
    }

    private void createNode(NodeCreated nodeCreated, Bulk.Builder bulkBuilder) throws RuntimeException {
        NodeRepresentation node = nodeCreated.getDetails();
        createOrUpdateNode(node, bulkBuilder);
    }

    private void updateNode(NodeUpdated nodeUpdated, Bulk.Builder bulkBuilder) {
        NodeRepresentation node = nodeUpdated.getDetails().getCurrent();
        createOrUpdateNode(node, bulkBuilder);
    }

    private void createOrUpdateNode(NodeRepresentation node, Bulk.Builder bulkBuilder) {
        String id = getKey(node);

        Map<String, String> source = new LinkedHashMap<>();
        for (String key : node.getProperties().keySet()) {
            source.put(key, String.valueOf(node.getProperties().get(key)));
        }

        for (String label : node.getLabels()) {
            bulkBuilder.addAction(new Index.Builder(source).index(getIndex()).type(label).id(id).build());
        }
    }

    private boolean deleteNode(NodeDeleted nodeDeleted, Bulk.Builder bulkBuilder) {
        NodeRepresentation node = nodeDeleted.getDetails();
        String id = getKey(node);

        boolean success = true;
        for (String label : node.getLabels()) {
            bulkBuilder.addAction(new Delete.Builder(id).index(getIndex()).type(label).build());
        }

        return success;
    }
    
    private Bulk.Builder startBulkOperation() {
        return new Bulk.Builder().defaultIndex(getIndex());
    }
    
    private boolean executeBulk(Bulk.Builder bulkBuilder) {
      Bulk bulkOperation = bulkBuilder.build();
      try {
            JestResult execute = getClient().execute(bulkOperation);
            if (!execute.isSucceeded()) {
                LOG.warn("Failed to execute an action against Elasticsearch. Details: " + execute.getErrorMessage());
            }
            return execute.isSucceeded();
        } catch (IOException e) {
            LOG.warn("Failed to execute an action against Elasticsearch. ", e);
            return false;
        }
    }
}
