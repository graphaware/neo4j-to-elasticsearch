/*
 * Copyright (c) 2013-2015 GraphAware
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

import com.graphaware.writer.thirdparty.NodeCreated;
import com.graphaware.writer.thirdparty.NodeDeleted;
import com.graphaware.writer.thirdparty.NodeUpdated;
import com.graphaware.writer.thirdparty.WriteOperation;

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
     * Tell Elasticsearch a node has been created.
     *
     * @param nodeCreated operation.
     */
    void createNode(NodeCreated nodeCreated);

    /**
     * Tell Elasticsearch a node has been updated.
     *
     * @param nodeUpdated operation.
     */
    void updateNode(NodeUpdated nodeUpdated);

    /**
     * Tell Elasticsearch a node has been deleted.
     *
     * @param nodeDeleted operation.
     */
    void deleteNode(NodeDeleted nodeDeleted);

    /**
     * Finish executing operations. Guranteed to be called as the last method.
     *
     * @return write operations that have not been successful. Never <code>null</code>.
     */
    List<WriteOperation<?>> flush();
}
