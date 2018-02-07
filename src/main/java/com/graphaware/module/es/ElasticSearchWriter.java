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
import com.graphaware.module.es.executor.BulkOperationExecutorFactory;
import com.graphaware.module.es.executor.OperationExecutor;
import com.graphaware.module.es.executor.OperationExecutorFactory;
import com.graphaware.module.es.executor.RequestPerOperationExecutorFactory;
import com.graphaware.module.es.mapping.Mapping;
import com.graphaware.module.es.search.Searcher;
import com.graphaware.writer.thirdparty.BaseThirdPartyWriter;
import com.graphaware.writer.thirdparty.ThirdPartyWriter;
import com.graphaware.writer.thirdparty.WriteOperation;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import org.neo4j.logging.Log;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.springframework.util.Assert.notNull;

/**
 * A {@link ThirdPartyWriter} to Elasticsearch (https://www.elastic.co/).
 */
public class ElasticSearchWriter extends BaseThirdPartyWriter {
    public static final int MAXIMUM_CONSECUTIVE_ERRORS = 50;

    private static final Log LOG = LoggerFactory.getLogger(ElasticSearchWriter.class);

    private JestClient client;
    private int consecutiveErrors = 0;
    private final String protocol;
    private final String uri;
    private final String port;
    private final boolean retryOnError;
    private final OperationExecutorFactory executorFactory;
    private final AtomicBoolean indexExists = new AtomicBoolean(false); //this must be thread-safe
    private final String authUser;
    private final String authPassword;
    private final Mapping mapping;
    private final boolean async;

    public ElasticSearchWriter(ElasticSearchConfiguration configuration) {
        super(configuration.getQueueCapacity());

        notNull(configuration, "Configuration cannot be null");

        this.protocol = configuration.getProtocol();
        this.uri = configuration.getUri();
        this.port = configuration.getPort();
        this.retryOnError = configuration.isRetryOnError();
        this.executorFactory = configuration.isExecuteBulk() ? new BulkOperationExecutorFactory() : new RequestPerOperationExecutorFactory();
        this.authUser = configuration.getAuthUser();
        this.authPassword = configuration.getAuthPassword();
        this.mapping = configuration.getMapping();
        this.async = configuration.isAsyncIndexation();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        LOG.info("Starting Elasticsearch Writer...");

        super.start();
        client = createClient();
        createIndexIfNotExist();

        LOG.info("Started Elasticsearch Writer.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        LOG.info("Stopping Elasticsearch Writer...");

        super.stop();
        shutdownClient();

        LOG.info("Stopped Elasticsearch Writer.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void processOperations(List<Collection<WriteOperation<?>>> operationGroups) {
        createIndexIfNotExist();

        OperationExecutor executor = executorFactory.newExecutor(client);

        executor.start();

        int actionsCount = 0;
        for (Collection<WriteOperation<?>> operationGroup : operationGroups) {
            for (WriteOperation<?> operation : operationGroup) {
                List<BulkableAction<? extends JestResult>> actions = mapping.getActions(operation);
                executor.execute(actions, operation);
                actionsCount += actions.size();
            }
        }

        if (actionsCount == 0) {
            return;
        }

        List<WriteOperation<?>> allFailed = executor.flush();

        // no errors, reset consecutive error counter
        if (allFailed.isEmpty()) {
            consecutiveErrors = 0;
            return;
        }

        // error, count consecutive errors
        consecutiveErrors++;

        // no retry-on-error, log and give up
        if (!retryOnError) {
            LOG.warn(allFailed.size() + " operations could not be replicated to Elasticsearch. These updates got lost.");
            return;
        }

        // retry-on-error enabled but maximum consecutive errors reached: give up and reset counter
        if (consecutiveErrors > MAXIMUM_CONSECUTIVE_ERRORS) {
            LOG.warn("" +
                    allFailed.size() + " operations could not be replicated to Elasticsearch. " +
                    "Giving up after " + MAXIMUM_CONSECUTIVE_ERRORS + " consecutive errors. " +
                    "These updated got lost."
            );
            consecutiveErrors = 0;
            return;
        }

        // retry-on-error enabled
        LOG.warn("" +
                allFailed.size() + " operations could not be replicated to Elasticsearch. " +
                (consecutiveErrors > 1 ? consecutiveErrors + " consecutive errors. "  : "") +
                "Will retry..."
        );
        retry(Collections.singletonList(allFailed));
        try {
            LOG.info("Backing off for 2 seconds...");
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            LOG.warn("Wait interrupted", e);
        }
    }

    protected JestClient createClient() {
        return Searcher.createClient(protocol, uri, port, authUser, authPassword);
    }

    protected void shutdownClient() {
        LOG.info("Shutting down Jest Client...");

        if (client != null) {
            client.shutdownClient();
            client = null;
        }

        LOG.info("Shut down Jest Client.");
    }

    protected void createIndexIfNotExist() {
        if (indexExists.get()) {
            return;
        }

        synchronized (this) {
            if (indexExists.get()) {
                return;
            }

            try {
                mapping.createIndexAndMapping(client);
                indexExists.set(true);
            } catch (Exception e) {
                LOG.error("Failed to create Elasticsearch index.", e);
            }
        }
    }
}
