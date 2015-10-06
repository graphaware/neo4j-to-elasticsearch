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

    /**
     * {@inheritDoc}
     */
    @Override
    public RuntimeModule bootstrapModule(String moduleId, Map<String, String> config, GraphDatabaseService database) {
        String uri, port;

        if (configExists(config, URI)) {
            uri = config.get(URI);
            LOG.info("Elasticsearch URI set to {}", uri);
        }
        else {
            LOG.error("Elasticsearch URI must be specified!");
            throw new IllegalStateException("Elasticsearch URI must be specified!");
        }

        if (configExists(config, PORT)) {
            port = config.get(PORT);
            LOG.info("Elasticsearch port set to {}", port);
        }
        else {
            LOG.error("Elasticsearch port must be specified!");
            throw new IllegalStateException("Elasticsearch port must be specified!");
        }

        ElasticSearchConfiguration configuration = ElasticSearchConfiguration.defaultConfiguration(uri, port);

        if (configExists(config, INDEX)) {
            configuration = configuration.withIndexName(config.get(INDEX));
            LOG.info("Elasticsearch index set to {}", configuration.getIndexName());
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

        if (configExists(config, NODES)) {
            NodeInclusionPolicy policy = StringToNodeInclusionPolicy.getInstance().apply(config.get(NODES));
            LOG.info("Node Inclusion Policy set to {}", policy);
            configuration = configuration.with(policy);
        }

        if (configExists(config, PROPERTIES)) {
            NodePropertyInclusionPolicy policy = StringToNodePropertyInclusionPolicy.getInstance().apply(config.get(PROPERTIES));
            LOG.info("Node Properties Inclusion Policy set to {}", policy);
            configuration = configuration.with(policy);
        }

        return new ElasticSearchModule(moduleId, configuration);
    }

    private boolean configExists(Map<String, String> config, String key) {
        return config.get(key) != null && config.get(key).length() > 0;
    }
}
