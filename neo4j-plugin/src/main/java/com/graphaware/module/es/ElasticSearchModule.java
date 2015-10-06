package com.graphaware.module.es;

import com.graphaware.runtime.config.TxDrivenModuleConfiguration;
import com.graphaware.runtime.metadata.TxDrivenModuleMetadata;
import com.graphaware.runtime.module.thirdparty.WriterBasedThirdPartyIntegrationModule;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchModule extends WriterBasedThirdPartyIntegrationModule {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchModuleBootstrapper.class);

    private final ElasticSearchConfiguration config;

    public ElasticSearchModule(String moduleId, ElasticSearchConfiguration config) {
        super(moduleId, new ElasticSearchWriter(config.getQueueCapacity(), config.getEsUri(), config.getEsPort(), config.getKeyProperty(), config.getIndexName(), config.isRetryOnError()));
        this.config = config;
    }

    @Override
    public TxDrivenModuleConfiguration getConfiguration() {
        return config;
    }

    @Override
    public void initialize(GraphDatabaseService database) {
        //todo
    }

    @Override
    public void reinitialize(GraphDatabaseService database, TxDrivenModuleMetadata oldMetadata) {
        //todo
    }
}
