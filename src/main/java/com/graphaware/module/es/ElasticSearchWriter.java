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

import com.graphaware.module.es.executor.BulkOperationExecutorFactory;
import com.graphaware.module.es.executor.OperationExecutor;
import com.graphaware.module.es.executor.OperationExecutorFactory;
import com.graphaware.module.es.executor.RequestPerOperationExecutorFactory;
import com.graphaware.module.es.mapping.Mapping;
import com.graphaware.writer.thirdparty.BaseThirdPartyWriter;
import com.graphaware.writer.thirdparty.ThirdPartyWriter;
import com.graphaware.writer.thirdparty.WriteOperation;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.springframework.util.Assert.notNull;

/**
 * A {@link ThirdPartyWriter} to Elasticsearch (https://www.elastic.co/).
 */
public class ElasticSearchWriter extends BaseThirdPartyWriter {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchWriter.class);

    private JestClient client;
    private final String uri;
    private final String port;
    private final String keyProperty;
    private final String index;
    private final boolean retryOnError;
    private final OperationExecutorFactory executorFactory;
    private final AtomicBoolean indexExists = new AtomicBoolean(false); //this must be thread-safe
    private final String authUser;
    private final String authPassword;
    private final Mapping mapping;

    public ElasticSearchWriter(ElasticSearchConfiguration configuration) {
        super(configuration.getQueueCapacity());

        notNull(configuration);

        this.uri = configuration.getUri();
        this.port = configuration.getPort();
        this.keyProperty = configuration.getKeyProperty();
        this.index = configuration.getIndex();
        this.retryOnError = configuration.isRetryOnError();
        this.executorFactory = configuration.isExecuteBulk() ? new BulkOperationExecutorFactory() : new RequestPerOperationExecutorFactory();
        this.authUser = configuration.getAuthUser();
        this.authPassword = configuration.getAuthPassword();
        this.mapping = Mapping.getMapping(index, keyProperty, configuration.getMapping());
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

        for (Collection<WriteOperation<?>> operationGroup : operationGroups) {
            for (WriteOperation<?> operation : operationGroup) {
                executor.execute(mapping.getActions(operation), operation);
            }
        }

        List<WriteOperation<?>> allFailed = executor.flush();

        if (!allFailed.isEmpty()) {
            if (retryOnError) {
                LOG.warn("There were " + allFailed.size() + " failures in replicating to Elasticsearch. Will retry...");
                retry(Collections.singletonList(allFailed));
                try {
                    LOG.info("Backing off for 2 seconds...");
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    LOG.warn("Wait interrupted", e);
                }
            } else {
                LOG.warn("There were " + allFailed.size() + " failures in replicating to Elasticsearch. These updates got lost.");
            }
        }
    }

    protected JestClient createClient() {
        LOG.info("Creating Jest Client...");

        JestClientFactory factory = new JestClientFactory();
        String esHost = String.format("http://%s:%s", uri, port);
        HttpClientConfig.Builder clientConfigBuilder = 
                new HttpClientConfig.Builder(esHost).multiThreaded(true);
        if (authUser != null && authPassword != null) {
            BasicCredentialsProvider customCredentialsProvider = new BasicCredentialsProvider();
            customCredentialsProvider.setCredentials(
                new AuthScope(uri, Integer.parseInt(port)),
                new UsernamePasswordCredentials(authUser, authPassword));
            LOG.info("Enabling Auth for elasticsearch: " + authUser);
            clientConfigBuilder.credentialsProvider(customCredentialsProvider);
        }
        factory.setHttpClientConfig(clientConfigBuilder
                .build());

        LOG.info("Created Jest Client.");

        return factory.getObject();
    }

    protected void shutdownClient() {
        LOG.info("Shutting down Jest Client...");

        client.shutdownClient();
        client = null;

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
                mapping.createIndexAndMapping(client, index);
                indexExists.set(true);
            } catch (Exception e) {
                LOG.error("Failed to create Elasticsearch index.", e);
            }
        }
    }
}
