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

import com.graphaware.writer.thirdparty.WriteOperation;
import io.searchbox.client.JestClient;

import java.util.LinkedList;
import java.util.List;

import static org.springframework.util.Assert.notNull;

/**
 * Base class for {@link OperationExecutor} implementations.
 */
public abstract class BaseOperationExecutor implements OperationExecutor {

    private List<WriteOperation<?>> allFailed;

    private final JestClient client;

    /**
     * Construct a new executor.
     *
     * @param client      Jest client. Must not be <code>null</code>.
     */
    public BaseOperationExecutor(JestClient client) {
        notNull(client);

        this.client = client;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        allFailed = new LinkedList<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<WriteOperation<?>> flush() {
        return allFailed;
    }

    /**
     * Add a {@link WriteOperation} to the collection of failed operations.
     *
     * @param failed to add.
     */
    protected final void addFailed(WriteOperation<?> failed) {
        this.allFailed.add(failed);
    }

    /**
     * Clear maintained failed operations.
     */
    protected final void clearFailed() {
        allFailed.clear();
    }

    /**
     * @return Jest client for Elasticsearch.
     */
    protected JestClient getClient() {
        return client;
    }

}
