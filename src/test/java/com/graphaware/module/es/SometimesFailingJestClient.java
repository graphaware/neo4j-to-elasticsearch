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

import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.client.JestResultHandler;

import java.io.IOException;
import java.util.Random;
import java.util.Set;

public class SometimesFailingJestClient implements JestClient {

    private final Random random = new Random();
    private final JestClient wrapped;

    public SometimesFailingJestClient(JestClient wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public <T extends JestResult> T execute(Action<T> clientRequest) throws IOException {
        if (random.nextBoolean()) {
            throw new IOException("Deliberate testing excetption");
        }
        return wrapped.execute(clientRequest);
    }

    @Override
    public <T extends JestResult> void executeAsync(Action<T> action, JestResultHandler<? super T> jestResultHandler) {
        throw new UnsupportedOperationException("Didn't think we're using this method!");
    }

    @Override
    public void shutdownClient() {
        wrapped.shutdownClient();
    }

    @Override
    public void setServers(Set<String> servers) {
        wrapped.setServers(servers);
    }
}
