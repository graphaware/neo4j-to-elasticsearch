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

package com.graphaware.module.es;

import com.graphaware.module.es.executor.OperationExecutorFactory;
import io.searchbox.client.JestClient;

public class SometimesFailingElasticSearchWriter extends ElasticSearchWriter {

    public SometimesFailingElasticSearchWriter(ElasticSearchConfiguration configuration) {
        super(configuration);
    }

    public SometimesFailingElasticSearchWriter(String uri, String port, String keyProperty, String index, boolean retryOnError, OperationExecutorFactory executorFactory) {
        super(uri, port, keyProperty, index, retryOnError, executorFactory, null, null);
    }

    public SometimesFailingElasticSearchWriter(int queueCapacity, String uri, String port, String keyProperty, String index, boolean retryOnError, OperationExecutorFactory executorFactory) {
        super(queueCapacity, uri, port, keyProperty, index, retryOnError, executorFactory, null, null);
    }

    @Override
    protected JestClient createClient() {
        return new SometimesFailingJestClient(super.createClient());
    }
}
