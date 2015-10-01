package com.graphaware.module.es;

import com.graphaware.common.policy.InclusionPolicies;
import com.graphaware.runtime.config.BaseTxDrivenModuleConfiguration;

public class ElasticSearchConfiguration extends BaseTxDrivenModuleConfiguration<ElasticSearchConfiguration> {

      private static final String DEFAULT_UUID_PROPERTY = "uuid";

    private final String elasticSearchUri;
    private final String elasticSearchPort;
    private final String elasticSearchIndex;
    private final String keyProperty;
    private boolean mandatory = true;

    public ElasticSearchConfiguration(InclusionPolicies inclusionPolicies, String elasticSearchUri, String elasticSearchPort, String elasticSearchIndex, String keyProperty) {
        super(inclusionPolicies);
        this.elasticSearchUri = elasticSearchUri;
        this.elasticSearchPort = elasticSearchPort;
        this.elasticSearchIndex = elasticSearchIndex;
        this.keyProperty = keyProperty == null ? DEFAULT_UUID_PROPERTY : keyProperty;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElasticSearchConfiguration newInstance(InclusionPolicies inclusionPolicies) {
        return new ElasticSearchConfiguration(inclusionPolicies, this.elasticSearchUri, this.elasticSearchPort, this.elasticSearchIndex, this.keyProperty);
    }

    public String getElasticSearchUri() {
        return this.elasticSearchUri;
    }

    public String getElasticSearchPort() {
        return this.elasticSearchPort;
    }

    public String getElasticSearchIndex() {
        return this.elasticSearchIndex;
    }
    
    public boolean isMandatory()  {
        return mandatory;
    }
    public String getKeyProperty()  {
        return keyProperty;
    }
    
    
}