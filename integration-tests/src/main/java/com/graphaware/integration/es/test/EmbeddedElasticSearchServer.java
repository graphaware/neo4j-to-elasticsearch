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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;

public class EmbeddedElasticSearchServer implements ElasticSearchServer {
    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedElasticSearchServer.class);

    private ElasticSearchServerWrapper embeddedServer;

    public void start() {
        final String classpath = System.getProperty("classpath");
        LOG.warn("classpath: " + classpath);
        try
        {
            CustomClassLoader loader = new CustomClassLoader(classpath);
            Class<Object> loadedClass = (Class<Object>) loader.loadClass(ElasticSearchClusterRunnerWrapper.class.getCanonicalName());
            embeddedServer = (ElasticSearchServerWrapper) Proxy.newProxyInstance(this.getClass().getClassLoader(),
                    new Class[]
                            {
                                    ElasticSearchServerWrapper.class
                            },
                    new PassThroughProxyHandler(loadedClass.newInstance()));
            embeddedServer.startEmbeddedServer();
        }
        catch (Exception ex)
        {
            LOG.warn("Error while creating and starting client", ex);
        }
    }

    public void stop() {
        embeddedServer.stopEmbeddedServer();
        embeddedServer = null;
        System.gc();
    }
}
