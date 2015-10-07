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
public class ElasticSearchWriter extends BaseThirdPartyWriter {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchWriter.class);

    private JestClient client;
    private final String uri;
    private final String port;
    private final String keyProperty;
    private final String index;
    private final boolean retryOnError;

    public ElasticSearchWriter(String uri, String port, String keyProperty, String index, boolean retryOnError) {
        this.uri = uri;
        this.port = port;
        this.keyProperty = keyProperty;
        this.index = index;
        this.retryOnError = retryOnError;
    }

    public ElasticSearchWriter(int queueCapacity, String uri, String port, String keyProperty, String index, boolean retryOnError) {
        super(queueCapacity);
        this.uri = uri;
        this.port = port;
        this.keyProperty = keyProperty;
        this.index = index;
        this.retryOnError = retryOnError;
    }

    @Override
    public void start() {
        super.start();
        createClient();
        createIndexIfNotExist();
    }

    @Override
    public void stop() {
        super.stop();
        shutdownClient();
    }

    @Override
    protected void processOperations(List<Collection<WriteOperation<?>>> list) {
        List<Collection<WriteOperation<?>>> allFailed = new LinkedList<>();

        for (Collection<WriteOperation<?>> collection : list) {

            Collection<WriteOperation<?>> failed = new HashSet<>();

            for (WriteOperation<?> operation : collection) {
                switch (operation.getType()) {
                    case NODE_CREATED:
                        if (!createNode((NodeCreated) operation)) {
                            failed.add(operation);
                        }
                        break;
                    case NODE_UPDATED:
                        if (!updateNode((NodeUpdated) operation)) {
                            failed.add(operation);
                        }
                        break;
                    case NODE_DELETED:
                        if (!deleteNode((NodeDeleted) operation)) {
                            failed.add(operation);
                        }
                        break;
                    default:
                        LOG.warn("Unsupported operation " + operation.getType());
                }
            }

            if (!failed.isEmpty()) {
                allFailed.add(failed);
            }
        }

        if (retryOnError && !allFailed.isEmpty()) {
            retry(allFailed);
        }
    }

    protected String getKey(NodeRepresentation node) {
        return String.valueOf(node.getProperties().get(keyProperty));
    }

    protected String getIndex() {
        return index;
    }

    private boolean createNode(NodeCreated nodeCreated) throws RuntimeException {
        NodeRepresentation node = nodeCreated.getDetails();
        return createOrUpdateNode(node);
    }

    private boolean updateNode(NodeUpdated nodeUpdated) {
        NodeRepresentation node = nodeUpdated.getDetails().getCurrent();
        return createOrUpdateNode(node);
    }

    private boolean createOrUpdateNode(NodeRepresentation node) {
        String id = getKey(node);

        Map<String, String> source = new LinkedHashMap<>();
        for (String key : node.getProperties().keySet()) {
            source.put(key, String.valueOf(node.getProperties().get(key)));
        }

        boolean success = true;
        for (String label : node.getLabels()) {
            if (!execute(new Index.Builder(source).index(getIndex()).type(label).id(id).build(), id)) {
                success = false;
            }
        }

        return success;
    }

    private boolean deleteNode(NodeDeleted nodeDeleted) {
        NodeRepresentation node = nodeDeleted.getDetails();
        String id = getKey(node);

        boolean success = true;
        for (String label : node.getLabels()) {
            if (!execute(new Delete.Builder(id).index(getIndex()).type(label).build(), id)) {
                success = false;
            }
        }

        return success;
    }

    private boolean execute(Action<? extends JestResult> insert, String nodeId) {
        try {
            final JestResult execute = client.execute(insert);

            if (!execute.isSucceeded()) {
                LOG.warn("Failed to execute an action against Elasticsearch. Details: " + execute.getErrorMessage());
            }

            return execute.isSucceeded();
        } catch (IOException e) {
            LOG.warn("Failed to execute an action against Elasticsearch. ", e);
            return false;
        }
    }

    private void createClient() {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder(String.format("http://%s:%s", uri, port))
                .multiThreaded(true)
                .build());

        client = factory.getObject();
    }
    
    protected JestClient getClient() {
        return client;
    }
    
    protected boolean getRetryOnError() {
        return retryOnError;
    }

    private void shutdownClient() {
        client.shutdownClient();
        client = null;
    }

    private void createIndexIfNotExist() {
        try {
            if (client.execute(new IndicesExists.Builder(getIndex()).build()).isSucceeded()) {
                return;
            }
            final JestResult execute = client.execute(new CreateIndex.Builder(getIndex()).build());
            if (!execute.isSucceeded()) {
                LOG.error("Failed to create Elasticsearch index. Details: " + execute.getErrorMessage());
            }
        } catch (IOException e) {
            LOG.error("Failed to create Elasticsearch index.", e);
        }
    }
}
