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
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestResult;

import java.util.List;

/**
 * Executes operations against Elasticsearch.
 */
public interface OperationExecutor {

    /**
     * Start executing operations. Guaranteed to be called before any other methods.
     */
    void start();

    /**
     * Execute some actions in ElasticsSearch.
     *
     * @param actions actions to execute on ElasticSearch
     * @param operation the original Neo4j operation
     */
    void execute(List<BulkableAction<? extends JestResult>> actions, WriteOperation<?> operation);

    /**
     * Finish executing operations. Guaranteed to be called as the last method.
     *
     * @return write operations that have not been successful. Never <code>null</code>.
     */
    List<WriteOperation<?>> flush();
}
