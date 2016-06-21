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
import com.graphaware.common.policy.*;
import com.graphaware.common.policy.none.IncludeNoNodes;
import com.graphaware.common.policy.none.IncludeNoRelationships;
import com.graphaware.common.representation.NodeRepresentation;
import com.graphaware.common.representation.RelationshipRepresentation;
import com.graphaware.runtime.config.TxDrivenModuleConfiguration;
import com.graphaware.runtime.metadata.TxDrivenModuleMetadata;
import com.graphaware.runtime.module.thirdparty.WriterBasedThirdPartyIntegrationModule;
import com.graphaware.tx.executor.batch.IterableInputBatchTransactionExecutor;
import com.graphaware.tx.executor.batch.UnitOfWork;
import com.graphaware.tx.executor.input.AllNodes;
import com.graphaware.tx.executor.input.AllRelationships;
import com.graphaware.writer.thirdparty.NodeCreated;
import com.graphaware.writer.thirdparty.RelationshipCreated;
import com.graphaware.writer.thirdparty.ThirdPartyWriter;
import com.graphaware.writer.thirdparty.WriteOperation;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.springframework.util.Assert.notNull;

/**
 * A {@link WriterBasedThirdPartyIntegrationModule} that indexes Neo4j nodes and their properties in Elasticsearch.
 */
public class ElasticSearchModule extends WriterBasedThirdPartyIntegrationModule {

    private static final Log LOG = LoggerFactory.getLogger(ElasticSearchModuleBootstrapper.class);
    private static final int REINDEX_BATCH_SIZE = 1000;

    private final ElasticSearchConfiguration config;
    private boolean reindex = false; //this is checked in a single thread
    private boolean isReindexed = false;

    /**
     * Create a new module.
     *
     * @param moduleId ID of the module. Must not be <code>null</code> or empty.
     * @param writer   to use for integrating with Elasticsearch. Must not be <code>null</code>.
     * @param config   module config. Must not be <code>null</code>.
     */
    public ElasticSearchModule(String moduleId, ThirdPartyWriter writer, ElasticSearchConfiguration config) {
        super(moduleId, writer);
        notNull(config);
        this.config = config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TxDrivenModuleConfiguration getConfiguration() {
        return config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(GraphDatabaseService database) {
        super.start(database);

        //Must be after start - else the ES connection is not initialised.
        if (reindex) {
            reindex(database);
            reindex = false;
        }
        isReindexed = true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(GraphDatabaseService database) {
        if (shouldReIndex("index")) {
            reindex = true;
        } else {
            isReindexed = true;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reinitialize(GraphDatabaseService database, TxDrivenModuleMetadata oldMetadata) {
        if (shouldReIndex("re-index")) {
            reindex = true;
        }
    }

    public boolean isReindexCompleted() {
        return isReindexed;
    }

    private boolean shouldReIndex(String logMessage) {
        long initializeUntil = config.initializeUntil();
        long now = System.currentTimeMillis();

        if (initializeUntil > now) {
            LOG.info("InitializeUntil set to " + initializeUntil + " and it is " + now + ". Will " + logMessage + " the entire database...");
            return true;
        } else {
            LOG.info("InitializeUntil set to " + initializeUntil + " and it is " + now + ". Will NOT " + logMessage + " the entire database.");
            return false;
        }
    }

    private void reindex(GraphDatabaseService database) {
        final InclusionPolicies policies = getConfiguration().getInclusionPolicies();

        if (!(policies.getNodeInclusionPolicy() instanceof IncludeNoNodes)) {
            LOG.info("Re-indexing nodes...");
            reindexNodes(database);
        } else {
            LOG.info("Skipping nodes indexation.");
        }

        if (!(policies.getRelationshipInclusionPolicy() instanceof IncludeNoRelationships)) {
            LOG.info("Re-indexing relationships...");
            reindexRelationships(database);
        } else {
            LOG.info("Skipping relationships indexation.");
        }

        LOG.info("Finished re-indexing database.");
    }

    private void reindexNodes(GraphDatabaseService database) {
        final InclusionPolicies policies = getConfiguration().getInclusionPolicies();
        final NodeInclusionPolicy nodePolicy = policies.getNodeInclusionPolicy();
        final NodePropertyInclusionPolicy nodePropertyPolicy = policies.getNodePropertyInclusionPolicy();

        final Collection<WriteOperation<?>> operations = new HashSet<>();

        new IterableInputBatchTransactionExecutor<>(
                database,
                REINDEX_BATCH_SIZE,
                new AllNodes(database, REINDEX_BATCH_SIZE),
                new UnitOfWork<Node>() {
                    @Override
                    public void execute(GraphDatabaseService database, Node node, int batchNumber, int stepNumber) {
                        if (!nodePolicy.include(node)) {
                            return;
                        }

                        operations.add(new NodeCreated(
                                new NodeRepresentation(node, propertiesToInclude(node, nodePropertyPolicy))
                        ));

                        if (operations.size() >= REINDEX_BATCH_SIZE) {
                            LOG.info("Done " + REINDEX_BATCH_SIZE);
                            afterCommit(new HashSet<>(operations));
                            operations.clear();
                        }
                    }
                }
        ).execute();

        if (operations.size() > 0) {
            afterCommit(new HashSet<>(operations));
            operations.clear();
        }
    }

    private void reindexRelationships(GraphDatabaseService database) {
        final InclusionPolicies policies = getConfiguration().getInclusionPolicies();
        final RelationshipInclusionPolicy relPolicy = policies.getRelationshipInclusionPolicy();
        final RelationshipPropertyInclusionPolicy relPropertyPolicy = policies.getRelationshipPropertyInclusionPolicy();

        final Collection<WriteOperation<?>> operations = new HashSet<>();

        new IterableInputBatchTransactionExecutor<>(
                database,
                REINDEX_BATCH_SIZE,
                new AllRelationships(database, REINDEX_BATCH_SIZE),
                new UnitOfWork<Relationship>() {
                    @Override
                    public void execute(GraphDatabaseService database, Relationship rel, int batchNumber, int stepNumber) {
                        if (!relPolicy.include(rel)) {
                            return;
                        }

                        operations.add(new RelationshipCreated(
                                new RelationshipRepresentation(rel, propertiesToInclude(rel, relPropertyPolicy))
                        ));

                        if (operations.size() >= REINDEX_BATCH_SIZE) {
                            LOG.info("Done " + REINDEX_BATCH_SIZE);
                            afterCommit(new HashSet<>(operations));
                            operations.clear();
                        }
                    }
                }
        ).execute();

        if (operations.size() > 0) {
            afterCommit(new HashSet<>(operations));
            operations.clear();
        }
    }

    private <T extends PropertyContainer> String[] propertiesToInclude(T item, PropertyInclusionPolicy<T> propertyInclusionPolicy) {
        Set<String> includedProps = new HashSet<>();
        for (String key : item.getPropertyKeys()) {
            if (propertyInclusionPolicy.include(key, item)) {
                includedProps.add(key);
            }
        }
        return includedProps.toArray(new String[includedProps.size()]);
    }
}
