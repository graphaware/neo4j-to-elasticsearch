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

package com.graphaware.integration.es.test;

import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;

import java.io.IOException;

public class JestElasticSearchClient implements ElasticSearchClient {

    private final JestClient wrapped;

    public JestElasticSearchClient(String host, String port) {
        String conn = String.format("http://%s:%s", host, port);
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder(conn)
                .multiThreaded(true)
                .build());
        this.wrapped = factory.getObject();
    }

    @Override
    public <T extends JestResult> T execute(Action<T> clientRequest) {
        try {
            return wrapped.execute(clientRequest);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void shutdown() {
        wrapped.shutdownClient();
    }
}
