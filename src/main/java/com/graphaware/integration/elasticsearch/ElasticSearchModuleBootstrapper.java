package com.graphaware.integration.elasticsearch;

import com.graphaware.runtime.module.RuntimeModule;
import com.graphaware.runtime.module.RuntimeModuleBootstrapper;
import com.graphaware.runtime.policy.InclusionPoliciesFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * {@link RuntimeModuleBootstrapper} that bootstraps {@link ElasticSearchModule}.
 */
public class ElasticSearchModuleBootstrapper implements RuntimeModuleBootstrapper {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchModuleBootstrapper.class);

    private static final String ES_URI = "uri";
    private static final String ES_PORT = "port";
    private static final String ES_INDEX = "index";

    /**
     * {@inheritDoc}
     */
    @Override
    public RuntimeModule bootstrapModule(String moduleId, Map<String, String> config, GraphDatabaseService database) {
        ElasticSearchConfiguration configuration = new ElasticSearchConfiguration(InclusionPoliciesFactory.allBusiness(), config.get(ES_URI), config.get(ES_PORT), config.get(ES_INDEX));

        return new ElasticSearchModule(moduleId, configuration, database);
    }
}
