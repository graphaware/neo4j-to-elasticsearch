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

import com.graphaware.runtime.module.BaseRuntimeModuleBootstrapper;
import com.graphaware.runtime.module.RuntimeModule;
import com.graphaware.runtime.module.RuntimeModuleBootstrapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * {@link RuntimeModuleBootstrapper} that bootstraps {@link ElasticSearchModule}.
 */
public class ElasticSearchModuleBootstrapper extends BaseRuntimeModuleBootstrapper<ElasticSearchConfiguration> {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchModuleBootstrapper.class);

    private static final String URI = "uri";
    private static final String PORT = "port";
    private static final String INDEX = "index";
    private static final String KEY_PROPERTY = "keyProperty";
    private static final String RETRY_ON_ERROR = "retryOnError";
    private static final String QUEUE_CAPACITY = "queueSize";
    private static final String BULK = "bulk";
    private static final String AUTH_USER = "authUser";
    private static final String AUTH_PASSWORD = "authPassword";
    private static final String MAPPING = "mapping";

    @Override
    protected ElasticSearchConfiguration defaultConfiguration() {
        return ElasticSearchConfiguration.defaultConfiguration();
    }

    @Override
    protected RuntimeModule doBootstrapModule(String moduleId, Map<String, String> config, GraphDatabaseService database, ElasticSearchConfiguration configuration) {
        if (configExists(config, URI)) {
            configuration = configuration.withUri(config.get(URI));
            LOG.info("Elasticsearch URI set to {}", configuration.getUri());
        } else {
            LOG.error("Elasticsearch URI must be specified!");
            throw new IllegalStateException("Elasticsearch URI must be specified!");
        }

        if (configExists(config, PORT)) {
            configuration = configuration.withPort(config.get(PORT));
            LOG.info("Elasticsearch port set to {}", configuration.getPort());
        } else {
            LOG.error("Elasticsearch port must be specified!");
            throw new IllegalStateException("Elasticsearch port must be specified!");
        }

        if (configExists(config, INDEX)) {
            configuration = configuration.withIndexName(config.get(INDEX));
            LOG.info("Elasticsearch index set to {}", configuration.getIndex());
        }

        if (configExists(config, KEY_PROPERTY)) {
            configuration = configuration.withKeyProperty(config.get(KEY_PROPERTY));
            LOG.info("Elasticsearch key property set to {}", configuration.getKeyProperty());
        }

        if (configExists(config, RETRY_ON_ERROR)) {
            configuration = configuration.withRetryOnError(Boolean.valueOf(config.get(RETRY_ON_ERROR)));
            LOG.info("Elasticsearch retry-on-error set to {}", configuration.isRetryOnError());
        }

        if (configExists(config, QUEUE_CAPACITY)) {
            configuration = configuration.withQueueCapacity(Integer.valueOf(config.get(QUEUE_CAPACITY)));
            LOG.info("Elasticsearch module queue capacity set to {}", configuration.getQueueCapacity());
        }

        if (configExists(config, BULK)) {
            configuration = configuration.withExecuteBulk(Boolean.valueOf(config.get(BULK)));
            LOG.info("Elasticsearch bulk execution set to {}", configuration.isExecuteBulk());
        }

        if (configExists(config, AUTH_USER) && configExists(config, AUTH_PASSWORD)) {
            configuration = configuration.withAuthCredentials(config.get(AUTH_USER), config.get(AUTH_PASSWORD));
            LOG.info("Elasticsearch Auth Credentials bulk execution set to {}", configuration.isExecuteBulk());
        }

        if (configExists(config, MAPPING)) {
            configuration = configuration.withMapping(config.get(MAPPING));
            LOG.info("Elasticsearch mapping set to {}", configuration.getMapping());
        }

        return new ElasticSearchModule(moduleId, produceWriter(configuration), configuration);
    }

    protected ElasticSearchWriter produceWriter(ElasticSearchConfiguration esConf) {
        return new ElasticSearchWriter(esConf);
    }
}
