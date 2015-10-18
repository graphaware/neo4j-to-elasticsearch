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
 *
 * You should have received a copy of the GNU General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.es.executor;

import com.graphaware.common.representation.NodeRepresentation;
import com.graphaware.writer.thirdparty.NodeCreated;
import com.graphaware.writer.thirdparty.NodeDeleted;
import com.graphaware.writer.thirdparty.NodeUpdated;
import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * {@link OperationExecutor} that executes each operation in a separate call.
 * <p/>
 * There should be a new instance of this class for each transaction. This class is not thread-safe and should be thrown
 * away after {@link #flush()} has been called.
 */
public class RequestPerOperationExecutor extends BaseOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(RequestPerOperationExecutor.class);

    public RequestPerOperationExecutor(JestClient client, String index, String keyProperty) {
        super(client, index, keyProperty);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createNode(NodeCreated nodeCreated) throws RuntimeException {
        if (!createOrUpdateNode(nodeCreated.getDetails())) {
            addFailed(nodeCreated);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateNode(NodeUpdated nodeUpdated) {
        if (!createOrUpdateNode(nodeUpdated.getDetails().getCurrent())) {
            addFailed(nodeUpdated);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteNode(NodeDeleted nodeDeleted) {
        NodeRepresentation node = nodeDeleted.getDetails();
        String id = getKey(node);

        for (String label : node.getLabels()) {
            if (!execute(new Delete.Builder(id).index(getIndex()).type(label).build(), id)) {
                addFailed(nodeDeleted);
            }
        }
    }

    private boolean createOrUpdateNode(NodeRepresentation node) {
        String id = getKey(node);

        Map<String, String> source = nodeToProps(node);

        boolean success = true;
        for (String label : node.getLabels()) {
            if (!execute(new Index.Builder(source).index(getIndex()).type(label).id(id).build(), id)) {
                success = false;
            }
        }

        return success;
    }

    private boolean execute(Action<? extends JestResult> insert, String nodeId) {
        try {
            final JestResult execute = getClient().execute(insert);

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
