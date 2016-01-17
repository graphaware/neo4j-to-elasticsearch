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

package com.graphaware.module.es.executor;

import com.graphaware.common.representation.NodeRepresentation;
import com.graphaware.writer.thirdparty.NodeCreated;
import com.graphaware.writer.thirdparty.NodeDeleted;
import com.graphaware.writer.thirdparty.NodeUpdated;
import com.graphaware.writer.thirdparty.WriteOperation;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * {@link OperationExecutor} that executes operations in bulk.
 * <p/>
 * There must be a new instance of this class for each transaction. This class is not thread-safe and must be thrown
 * away after {@link #flush()} has been called.
 */
public class BulkOperationExecutor extends BaseOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(BulkOperationExecutor.class);

    private Bulk.Builder bulkBuilder;

    /**
     * Construct a new executor.
     *
     * @param client      Jest client. Must not be <code>null</code>.
     * @param index       Elasticsearch index name. Must not be <code>null</code> or empty.
     * @param keyProperty name of the node property that serves as the key, under which the node will be indexed in Elasticsearch. Must not be <code>null</code> or empty.
     */
    public BulkOperationExecutor(JestClient client, String index, String keyProperty) {
        super(client, index, keyProperty);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        super.start();
        bulkBuilder = new Bulk.Builder().defaultIndex(getIndex());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<WriteOperation<?>> flush() {
        executeBulk(bulkBuilder);
        return super.flush();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createNode(NodeCreated nodeCreated) {
        createOrUpdateNode(nodeCreated.getDetails());
        addFailed(nodeCreated); //temporary
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateNode(NodeUpdated nodeUpdated) {
        createOrUpdateNode(nodeUpdated.getDetails().getCurrent());
        addFailed(nodeUpdated); //temporary
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteNode(NodeDeleted nodeDeleted) {
        NodeRepresentation node = nodeDeleted.getDetails();
        String id = getKey(node);

        for (String label : node.getLabels()) {
            bulkBuilder.addAction(new Delete.Builder(id).index(getIndex()).type(label).build());
        }

        addFailed(nodeDeleted); //temporary
    }

    private void createOrUpdateNode(NodeRepresentation node) {
        String id = getKey(node);

        Map<String, String> source = nodeToProps(node);

        for (String label : node.getLabels()) {
            bulkBuilder.addAction(new Index.Builder(source).index(getIndex()).type(label).id(id).build());
        }
    }

    private void executeBulk(Bulk.Builder bulkBuilder) {
        Bulk bulkOperation = bulkBuilder.build();
        try {
            JestResult execute = getClient().execute(bulkOperation);
            if (execute.isSucceeded()) {
                clearFailed();
            } else {
                LOG.warn("Failed to execute an action against Elasticsearch. Details: " + execute.getErrorMessage());
            }
        } catch (IOException e) {
            LOG.warn("Failed to execute an action against Elasticsearch. ", e);
        }
    }
}
