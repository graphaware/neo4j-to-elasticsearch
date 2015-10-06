package com.graphaware.module.es;

import com.graphaware.common.policy.InclusionPolicies;
import com.graphaware.common.policy.none.IncludeNoRelationships;
import com.graphaware.runtime.config.BaseTxDrivenModuleConfiguration;
import com.graphaware.runtime.policy.InclusionPoliciesFactory;

public class ElasticSearchConfiguration extends BaseTxDrivenModuleConfiguration<ElasticSearchConfiguration> {

    private static final String DEFAULT_KEY_PROPERTY = "uuid";
    private static final boolean DEFAULT_RETRY_ON_ERROR = false;
    private static final InclusionPolicies DEFAULT_INCLUSION_POLICIES = InclusionPoliciesFactory.allBusiness().with(IncludeNoRelationships.getInstance());
    private static final String DEFAULT_INDEX_NAME = "neo4j-index";
    private static final int DEFAULT_QUEUE_CAPACITY = 10000;

    private final String esUri;
    private final String esPort;
    private final String indexName;
    private final String keyProperty;
    private final boolean retryOnError;
    private final int queueCapacity;

    private ElasticSearchConfiguration(InclusionPolicies inclusionPolicies, String esUri, String esPort, String indexName, String keyProperty, boolean retryOnError, int queueCapacity) {
        super(inclusionPolicies);
        this.esUri = esUri;
        this.esPort = esPort;
        this.indexName = indexName;
        this.keyProperty = keyProperty;
        this.retryOnError = retryOnError;
        this.queueCapacity = queueCapacity;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElasticSearchConfiguration newInstance(InclusionPolicies inclusionPolicies) {
        return new ElasticSearchConfiguration(inclusionPolicies, getEsUri(), getEsPort(), getIndexName(), getKeyProperty(), isRetryOnError(), getQueueCapacity());
    }

    public static ElasticSearchConfiguration defaultConfiguration(String esUri, String esPort) {
        return new ElasticSearchConfiguration(DEFAULT_INCLUSION_POLICIES, esUri, esPort, DEFAULT_INDEX_NAME, DEFAULT_KEY_PROPERTY, DEFAULT_RETRY_ON_ERROR, DEFAULT_QUEUE_CAPACITY);
    }

    public ElasticSearchConfiguration withIndexName(String indexName) {
        return new ElasticSearchConfiguration(getInclusionPolicies(), getEsUri(), getEsPort(), indexName, getKeyProperty(), isRetryOnError(), getQueueCapacity());
    }

    public ElasticSearchConfiguration withKeyProperty(String keyProperty) {
        return new ElasticSearchConfiguration(getInclusionPolicies(), getEsUri(), getEsPort(), getIndexName(), keyProperty, isRetryOnError(), getQueueCapacity());
    }

    public ElasticSearchConfiguration withRetryOnError(boolean retryOnError) {
        return new ElasticSearchConfiguration(getInclusionPolicies(), getEsUri(), getEsPort(), getIndexName(), getKeyProperty(), retryOnError, getQueueCapacity());
    }

    public ElasticSearchConfiguration withQueueCapacity(int queueCapacity) {
        return new ElasticSearchConfiguration(getInclusionPolicies(), getEsUri(), getEsPort(), getIndexName(), getKeyProperty(), isRetryOnError(), queueCapacity);
    }

    public String getEsUri() {
        return esUri;
    }

    public String getEsPort() {
        return esPort;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getKeyProperty() {
        return keyProperty;
    }

    public boolean isRetryOnError() {
        return retryOnError;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        ElasticSearchConfiguration that = (ElasticSearchConfiguration) o;

        if (retryOnError != that.retryOnError) {
            return false;
        }
        if (queueCapacity != that.queueCapacity) {
            return false;
        }
        if (!esUri.equals(that.esUri)) {
            return false;
        }
        if (!esPort.equals(that.esPort)) {
            return false;
        }
        if (!indexName.equals(that.indexName)) {
            return false;
        }
        return keyProperty.equals(that.keyProperty);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + esUri.hashCode();
        result = 31 * result + esPort.hashCode();
        result = 31 * result + indexName.hashCode();
        result = 31 * result + keyProperty.hashCode();
        result = 31 * result + (retryOnError ? 1 : 0);
        result = 31 * result + queueCapacity;
        return result;
    }
}