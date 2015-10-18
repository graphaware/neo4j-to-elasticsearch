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
 *
 * You should have received a copy of the GNU General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.es;

import com.graphaware.common.policy.NodeInclusionPolicy;
import com.graphaware.common.policy.NodePropertyInclusionPolicy;
import com.graphaware.runtime.config.function.StringToNodeInclusionPolicy;
import com.graphaware.runtime.config.function.StringToNodePropertyInclusionPolicy;
import com.graphaware.runtime.module.RuntimeModule;
import com.graphaware.runtime.module.RuntimeModuleBootstrapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * {@link RuntimeModuleBootstrapper} that bootstraps {@link ElasticSearchModule}.
 */
public class ElasticSearchModuleBootstrapper implements RuntimeModuleBootstrapper {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchModuleBootstrapper.class);

    private static final String URI = "uri";
    private static final String PORT = "port";
    private static final String INDEX = "index";
    private static final String KEY_PROPERTY = "keyProperty";
    private static final String RETRY_ON_ERROR = "retryOnError";
    private static final String NODES = "nodes";
    private static final String PROPERTIES = "properties";
    private static final String QUEUE_CAPACITY = "queueSize";
    private static final String BULK = "bulk";
    private static final String REINDEX_UNTIL = "reindexUntil";

    /**
     * {@inheritDoc}
     */
    @Override
    public RuntimeModule bootstrapModule(String moduleId, Map<String, String> config, GraphDatabaseService database) {
        String uri, port;

        if (configExists(config, URI)) {
            uri = config.get(URI);
            LOG.info("Elasticsearch URI set to {}", uri);
        } else {
            LOG.error("Elasticsearch URI must be specified!");
            throw new IllegalStateException("Elasticsearch URI must be specified!");
        }

        if (configExists(config, PORT)) {
            port = config.get(PORT);
            LOG.info("Elasticsearch port set to {}", port);
        } else {
            LOG.error("Elasticsearch port must be specified!");
            throw new IllegalStateException("Elasticsearch port must be specified!");
        }

        ElasticSearchConfiguration esConf = ElasticSearchConfiguration.defaultConfiguration(uri, port);

        if (configExists(config, INDEX)) {
            esConf = esConf.withIndexName(config.get(INDEX));
            LOG.info("Elasticsearch index set to {}", esConf.getIndex());
        }

        if (configExists(config, KEY_PROPERTY)) {
            esConf = esConf.withKeyProperty(config.get(KEY_PROPERTY));
            LOG.info("Elasticsearch key property set to {}", esConf.getKeyProperty());
        }

        if (configExists(config, RETRY_ON_ERROR)) {
            esConf = esConf.withRetryOnError(Boolean.valueOf(config.get(RETRY_ON_ERROR)));
            LOG.info("Elasticsearch retry-on-error set to {}", esConf.isRetryOnError());
        }

        if (configExists(config, QUEUE_CAPACITY)) {
            esConf = esConf.withQueueCapacity(Integer.valueOf(config.get(QUEUE_CAPACITY)));
            LOG.info("Elasticsearch module queue capacity set to {}", esConf.getQueueCapacity());
        }

        if (configExists(config, NODES)) {
            NodeInclusionPolicy policy = StringToNodeInclusionPolicy.getInstance().apply(config.get(NODES));
            LOG.info("Node Inclusion Policy set to {}", policy);
            esConf = esConf.with(policy);
        }

        if (configExists(config, PROPERTIES)) {
            NodePropertyInclusionPolicy policy = StringToNodePropertyInclusionPolicy.getInstance().apply(config.get(PROPERTIES));
            LOG.info("Node Properties Inclusion Policy set to {}", policy);
            esConf = esConf.with(policy);
        }

        if (configExists(config, BULK)) {
            esConf = esConf.withExecuteBulk(Boolean.valueOf(config.get(BULK)));
            LOG.info("Elasticsearch bulk execution set to {}", esConf.isExecuteBulk());
        }

        if (configExists(config, REINDEX_UNTIL)) {
            esConf = esConf.withReindexUntil(Long.valueOf(config.get(REINDEX_UNTIL)));
            LOG.info("Elasticsearch re-index until set to {}", esConf.getReindexUntil());
            if (esConf.getReindexUntil() != 0) {
                long now = System.currentTimeMillis();
                LOG.info("That's " + Math.abs(now - esConf.getReindexUntil()) + " ms in the " + (now > esConf.getReindexUntil() ? "past" : "future"));
            }
        }

        return new ElasticSearchModule(moduleId, produceWriter(esConf), esConf);
    }

    protected ElasticSearchWriter produceWriter(ElasticSearchConfiguration esConf) {
        return new ElasticSearchWriter(esConf);
    }

    private boolean configExists(Map<String, String> config, String key) {
        return config.get(key) != null && config.get(key).length() > 0;
    }
}
