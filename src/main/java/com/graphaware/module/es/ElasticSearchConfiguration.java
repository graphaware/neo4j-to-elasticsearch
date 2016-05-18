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

import com.graphaware.common.policy.InclusionPolicies;
import com.graphaware.common.policy.none.IncludeNoRelationships;
import com.graphaware.runtime.config.BaseTxDrivenModuleConfiguration;
import com.graphaware.runtime.policy.InclusionPoliciesFactory;

/**
 * {@link BaseTxDrivenModuleConfiguration} for {@link ElasticSearchModule}.
 */
public class ElasticSearchConfiguration extends BaseTxDrivenModuleConfiguration<ElasticSearchConfiguration> {

    private static final String DEFAULT_KEY_PROPERTY = "uuid";
    private static final boolean DEFAULT_RETRY_ON_ERROR = false;
    private static final boolean DEFAULT_EXECUTE_BULK = true;
    private static final InclusionPolicies DEFAULT_INCLUSION_POLICIES = InclusionPoliciesFactory.allBusiness().with(IncludeNoRelationships.getInstance());
    private static final String DEFAULT_INDEX_NAME = "neo4j-index";
    private static final int DEFAULT_QUEUE_CAPACITY = 10000;
    private static final String DEFAULT_AUTH_USER = null;
    private static final String DEFAULT_AUTH_PASSWORD = null;

    private final String uri;
    private final String port;
    private final String index;
    private final String keyProperty;
    private final boolean retryOnError;
    private final int queueCapacity;
    private final boolean executeBulk;
    private final String authUser;
    private final String authPassword;
    

    /**
     * Construct a new configuration.
     *
     * @param inclusionPolicies specifying which nodes and node properties to index in Elasticsearch. Must not be <code>null</code>.
     * @param initializeUntil   until what time in ms since epoch it is ok to re-index the entire database in case the configuration has changed since
     *                          the last time the module was started, or if it is the first time the module was registered.
     *                          0 for never. The purpose of this is not to re-index all the time if the user is unaware.
     * @param uri               Elasticsearch URI. Must not be <code>null</code>.
     * @param port              Elasticsearch port. Must not be <code>null</code>.
     * @param index             name of the Elasticsearch index. Must not be <code>null</code> or empty.
     * @param keyProperty       name of the node property that serves as the key, under which the node will be indexed in Elasticsearch. Must not be <code>null</code> or empty.
     * @param retryOnError      whether to retry an index update after a failure (<code>true</code>) or throw the update away (<code>false</code>).
     * @param queueCapacity     capacity of the queue holding operations to be written to Elasticsearch. Must be positive.
     * @param executeBulk       whether or not to execute updates against Elasticsearch in bulk. It is recommended to set this to <code>true</code>.*
     */
    private ElasticSearchConfiguration(InclusionPolicies inclusionPolicies, long initializeUntil, String uri, String port, String index, String keyProperty, boolean retryOnError, int queueCapacity, boolean executeBulk, String authUser, String authPassword) {
        super(inclusionPolicies, initializeUntil);
        this.uri = uri;
        this.port = port;
        this.index = index;
        this.keyProperty = keyProperty;
        this.retryOnError = retryOnError;
        this.queueCapacity = queueCapacity;
        this.executeBulk = executeBulk;
        this.authUser = authUser;
        this.authPassword = authPassword;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElasticSearchConfiguration newInstance(InclusionPolicies inclusionPolicies, long initializeUntil) {
        return new ElasticSearchConfiguration(inclusionPolicies, initializeUntil, getUri(), getPort(), getIndex(), getKeyProperty(), isRetryOnError(), getQueueCapacity(), isExecuteBulk(), DEFAULT_AUTH_USER, DEFAULT_AUTH_PASSWORD);
    }

    public static ElasticSearchConfiguration defaultConfiguration() {
        return new ElasticSearchConfiguration(DEFAULT_INCLUSION_POLICIES, NEVER, null, null, DEFAULT_INDEX_NAME, DEFAULT_KEY_PROPERTY, DEFAULT_RETRY_ON_ERROR, DEFAULT_QUEUE_CAPACITY, DEFAULT_EXECUTE_BULK, DEFAULT_AUTH_USER, DEFAULT_AUTH_PASSWORD);
    }

    public ElasticSearchConfiguration withUri(String uri) {
        return new ElasticSearchConfiguration(getInclusionPolicies(), initializeUntil(), uri, getPort(), getIndex(), getKeyProperty(), isRetryOnError(), getQueueCapacity(), isExecuteBulk(), getAuthUser(), getAuthPassword());
    }

    public ElasticSearchConfiguration withPort(String port) {
        return new ElasticSearchConfiguration(getInclusionPolicies(), initializeUntil(), getUri(), port, getIndex(), getKeyProperty(), isRetryOnError(), getQueueCapacity(), isExecuteBulk(), getAuthUser(), getAuthPassword());
    }

    public ElasticSearchConfiguration withIndexName(String indexName) {
        return new ElasticSearchConfiguration(getInclusionPolicies(), initializeUntil(), getUri(), getPort(), indexName, getKeyProperty(), isRetryOnError(), getQueueCapacity(), isExecuteBulk(), getAuthUser(), getAuthPassword());
    }

    public ElasticSearchConfiguration withKeyProperty(String keyProperty) {
        return new ElasticSearchConfiguration(getInclusionPolicies(), initializeUntil(), getUri(), getPort(), getIndex(), keyProperty, isRetryOnError(), getQueueCapacity(), isExecuteBulk(), getAuthUser(), getAuthPassword());
    }

    public ElasticSearchConfiguration withRetryOnError(boolean retryOnError) {
        return new ElasticSearchConfiguration(getInclusionPolicies(), initializeUntil(), getUri(), getPort(), getIndex(), getKeyProperty(), retryOnError, getQueueCapacity(), isExecuteBulk(), getAuthUser(), getAuthPassword());
    }

    public ElasticSearchConfiguration withQueueCapacity(int queueCapacity) {
        return new ElasticSearchConfiguration(getInclusionPolicies(), initializeUntil(), getUri(), getPort(), getIndex(), getKeyProperty(), isRetryOnError(), queueCapacity, isExecuteBulk(), getAuthUser(), getAuthPassword());
    }

    public ElasticSearchConfiguration withExecuteBulk(boolean executeBulk) {
        return new ElasticSearchConfiguration(getInclusionPolicies(), initializeUntil(), getUri(), getPort(), getIndex(), getKeyProperty(), isRetryOnError(), getQueueCapacity(), executeBulk, getAuthUser(), getAuthPassword());
    }
    
    public ElasticSearchConfiguration withAuthCredentials(String authUser, String authPassword) {
        return new ElasticSearchConfiguration(getInclusionPolicies(), initializeUntil(), getUri(), getPort(), getIndex(), getKeyProperty(), isRetryOnError(), getQueueCapacity(), isExecuteBulk(), authUser, authPassword);
    }

    public String getUri() {
        return uri;
    }

    public String getPort() {
        return port;
    }

    public String getIndex() {
        return index;
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

    public boolean isExecuteBulk() {
        return executeBulk;
    }
    
    public String getAuthUser() {
      return authUser;
    }
    
    public String getAuthPassword() {    
      return authPassword;
    }
    
    /**
     * {@inheritDoc}
     */
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

        if (initializeUntil() != that.initializeUntil()) { //a bit of a hack to force initialization if this changes
            return false;
        }
        if (retryOnError != that.retryOnError) {
            return false;
        }
        if (queueCapacity != that.queueCapacity) {
            return false;
        }
        if (executeBulk != that.executeBulk) {
            return false;
        }
        if (!uri.equals(that.uri)) {
            return false;
        }
        if (!port.equals(that.port)) {
            return false;
        }
        if (!index.equals(that.index)) {
            return false;
        }
        return keyProperty.equals(that.keyProperty);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) (initializeUntil() ^ (initializeUntil() >>> 32));
        result = 31 * result + uri.hashCode();
        result = 31 * result + port.hashCode();
        result = 31 * result + index.hashCode();
        result = 31 * result + keyProperty.hashCode();
        result = 31 * result + (retryOnError ? 1 : 0);
        result = 31 * result + queueCapacity;
        result = 31 * result + (executeBulk ? 1 : 0);
        return result;
    }
}