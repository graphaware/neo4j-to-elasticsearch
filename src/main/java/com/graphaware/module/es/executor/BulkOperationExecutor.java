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

import com.graphaware.common.log.LoggerFactory;
import com.graphaware.writer.thirdparty.WriteOperation;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.util.List;

/**
 * {@link OperationExecutor} that executes operations in bulk.
 * <p/>
 * There must be a new instance of this class for each transaction. This class is not thread-safe and must be thrown
 * away after {@link #flush()} has been called.
 */
public class BulkOperationExecutor extends BaseOperationExecutor {

    private static final Log LOG = LoggerFactory.getLogger(BulkOperationExecutor.class);

    private Bulk.Builder bulkBuilder;

    /**
     * Construct a new executor.
     *
     * @param client      Jest client. Must not be <code>null</code>.
     */
    public BulkOperationExecutor(JestClient client) {
        super(client);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        super.start();
        bulkBuilder = new Bulk.Builder();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<WriteOperation<?>> flush() {
        executeBulk(bulkBuilder);
        return super.flush();
    }

    @Override
    public void execute(List<BulkableAction<? extends JestResult>> actions, WriteOperation<?> operation) {
        for (BulkableAction<? extends JestResult> action : actions) {
            bulkBuilder.addAction(action);
        }
        // Add all operations to the failed list.  They will be removed if "flush" succeeds.
        addFailed(operation);
    }

    public void reset() {
        bulkBuilder = new Bulk.Builder();
    }

    private void executeBulk(Bulk.Builder bulkBuilder) {
        Bulk bulkOperation = bulkBuilder.build();
        try {
            JestResult execute = getClient().execute(bulkOperation);
            if (execute.isSucceeded()) {
                // Removing all operation from the failed list if the bulk execution succeeded.
                clearFailed();
                LOG.info("Bulk operation succeeded");
            } else {
                LOG.warn("Failed to execute bulk action against ElasticSearch. Details: " + execute.getErrorMessage());
            }
        } catch (IOException e) {
            LOG.warn("Failed to execute bulk action against ElasticSearch. ", e);
        }
    }
}
