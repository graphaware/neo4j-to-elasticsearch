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

import com.graphaware.common.log.LoggerFactory;
import com.graphaware.module.es.mapping.Mapping;
import com.graphaware.module.es.util.ServiceLoader;
import com.graphaware.runtime.module.BaseRuntimeModuleBootstrapper;
import com.graphaware.runtime.module.RuntimeModule;
import com.graphaware.runtime.module.RuntimeModuleBootstrapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;

import java.util.Map;

/**
 * {@link RuntimeModuleBootstrapper} that bootstraps {@link ElasticSearchModule}.
 */
public class ElasticSearchModuleBootstrapper extends BaseRuntimeModuleBootstrapper<ElasticSearchConfiguration> {

    private static final Log LOG = LoggerFactory.getLogger(ElasticSearchModuleBootstrapper.class);

    private static final String PROTOCOL = "protocol";
    private static final String URI = "uri";
    private static final String PORT = "port";
    private static final String KEY_PROPERTY = "keyProperty";
    private static final String RETRY_ON_ERROR = "retryOnError";
    private static final String QUEUE_CAPACITY = "queueSize";
    private static final String REINDEX_BATCH_SIZE = "reindexBatchSize";
    private static final String BULK = "bulk";
    private static final String ASYNC_INDEXATION = "asyncIndexation";
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
            LOG.info("Elasticsearch URI set to %s", configuration.getUri());
        } else {
            LOG.error("Elasticsearch URI must be specified!");
            throw new IllegalStateException("Elasticsearch URI must be specified!");
        }

        if (configExists(config, PROTOCOL)) {
            configuration = configuration.withProtocol(config.get(PROTOCOL));
            LOG.info("Elasticsearch protocol set to %s", configuration.getProtocol());
        } else {
            LOG.error("Elasticsearch protocol set to default protocol http");
            configuration.withProtocol(ElasticSearchConfiguration.DEFAULT_PROTOCOL);
        }

        if (configExists(config, PORT)) {
            configuration = configuration.withPort(config.get(PORT));
            LOG.info("Elasticsearch port set to %s", configuration.getPort());
        } else {
            LOG.error("Elasticsearch port must be specified!");
            throw new IllegalStateException("Elasticsearch port must be specified!");
        }

        if (configExists(config, KEY_PROPERTY)) {
            configuration = configuration.withKeyProperty(config.get(KEY_PROPERTY));
            LOG.info("Elasticsearch key property set to %s", configuration.getKeyProperty());
        }

        if (configExists(config, RETRY_ON_ERROR)) {
            configuration = configuration.withRetryOnError(Boolean.valueOf(config.get(RETRY_ON_ERROR)));
            LOG.info("Elasticsearch retry-on-error set to %s", configuration.isRetryOnError());
        }

        if (configExists(config, QUEUE_CAPACITY)) {
            configuration = configuration.withQueueCapacity(Integer.valueOf(config.get(QUEUE_CAPACITY)));
            LOG.info("Elasticsearch module queue capacity set to %s", configuration.getQueueCapacity());
        }

        if (configExists(config, REINDEX_BATCH_SIZE)) {
            configuration = configuration.withReindexBatchSize(Integer.valueOf(config.get(REINDEX_BATCH_SIZE)));
            LOG.info("Elasticsearch module reindex batch size set to %s", configuration.getReindexBatchSize());
        }

        if (configExists(config, BULK)) {
            configuration = configuration.withExecuteBulk(Boolean.valueOf(config.get(BULK)));
            LOG.info("Elasticsearch bulk execution set to %s", configuration.isExecuteBulk());
        }

        if (configExists(config, ASYNC_INDEXATION)) {
            configuration = configuration.withAsyncIndexation(Boolean.valueOf(config.get(ASYNC_INDEXATION)));
            LOG.info("Elasticsearch async indexation set to %s", configuration.isAsyncIndexation());
        }

        if (configExists(config, AUTH_USER) && configExists(config, AUTH_PASSWORD)) {
            configuration = configuration.withAuthCredentials(config.get(AUTH_USER), config.get(AUTH_PASSWORD));
            LOG.info("Elasticsearch Auth Credentials bulk execution set to %s", configuration.isExecuteBulk());
        }

        String mappingClass = configExists(config, MAPPING) ? config.get(MAPPING) : "com.graphaware.module.es.mapping.DefaultMapping";
        Mapping mapping = ServiceLoader.loadMapping(mappingClass);
        configuration = configuration.withMapping(mapping, config);
        LOG.info("Elasticsearch mapping configured with %s", mapping.getClass());

        return new ElasticSearchModule(moduleId, produceWriter(configuration), configuration);
    }

    protected ElasticSearchWriter produceWriter(ElasticSearchConfiguration esConf) {
        return new ElasticSearchWriter(esConf);
    }
}
