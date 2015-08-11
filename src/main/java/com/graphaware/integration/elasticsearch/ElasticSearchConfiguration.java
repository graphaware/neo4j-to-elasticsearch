package com.graphaware.integration.elasticsearch;

import com.graphaware.common.policy.InclusionPolicies;
import com.graphaware.runtime.config.BaseTxDrivenModuleConfiguration;

public class ElasticSearchConfiguration extends BaseTxDrivenModuleConfiguration<ElasticSearchConfiguration> {

    private String elasticSearchUri;
    private String elasticSearchPort;
    private String elasticSearchIndex;

    public ElasticSearchConfiguration(InclusionPolicies inclusionPolicies, String elasticSearchUri, String elasticSearchPort, String elasticSearchIndex) {
        super(inclusionPolicies);
        this.elasticSearchUri = elasticSearchUri;
        this.elasticSearchPort = elasticSearchPort;
        this.elasticSearchIndex = elasticSearchIndex;
    }

    public ElasticSearchConfiguration newInstance(InclusionPolicies inclusionPolicies) {
        return new ElasticSearchConfiguration(inclusionPolicies, this.elasticSearchUri, this.elasticSearchPort, this.elasticSearchIndex);
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
}