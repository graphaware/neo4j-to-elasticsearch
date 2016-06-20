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
import io.searchbox.action.Action;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.util.List;

/**
 * {@link OperationExecutor} that executes each operation in a separate call.
 * <p/>
 * There should be a new instance of this class for each transaction. This class is not thread-safe and should be thrown
 * away after {@link #flush()} has been called.
 */
public class RequestPerOperationExecutor extends BaseOperationExecutor {

    private static final Log LOG = LoggerFactory.getLogger(RequestPerOperationExecutor.class);

    public RequestPerOperationExecutor(JestClient client) {
        super(client);
    }

    @Override
    public void execute(List<BulkableAction<? extends JestResult>> actions, WriteOperation<?> operation) {
        boolean success = true;
        for (BulkableAction<? extends JestResult> action : actions) {
            if (!execute(action)) {
                success = false;
                break;
            }
        }
        if (!success) {
            addFailed(operation);
        }
    }

    private boolean execute(Action<? extends JestResult> action) {
        try {
            final JestResult execute = getClient().execute(action);

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
