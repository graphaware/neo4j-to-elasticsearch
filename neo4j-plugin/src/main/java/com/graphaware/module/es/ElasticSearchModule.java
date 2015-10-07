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
        super(moduleId, elasticSearchWriterFactory(config));
        this.config = config;
    }
    
    public static ElasticSearchWriter elasticSearchWriterFactory(ElasticSearchConfiguration config)
    {
      if (config.isExecuteBulk())
        return new ElasticSearchBulkWriter(config.getQueueCapacity(), config.getEsUri(), config.getEsPort(), config.getKeyProperty(), config.getIndexName(), config.isRetryOnError());
      else
        return new ElasticSearchWriter(config.getQueueCapacity(), config.getEsUri(), config.getEsPort(), config.getKeyProperty(), config.getIndexName(), config.isRetryOnError());
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
