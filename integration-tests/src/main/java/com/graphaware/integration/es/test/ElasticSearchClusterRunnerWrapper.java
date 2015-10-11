package com.graphaware.integration.es.test;

import org.apache.log4j.Logger;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.common.settings.ImmutableSettings;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * {@link ElasticSearchServerWrapper} for testing that uses {@link ElasticsearchClusterRunner} to run an embedded ES server.
 */
public class ElasticSearchClusterRunnerWrapper implements ElasticSearchServerWrapper {
    private static final Logger LOG = Logger.getLogger(ElasticSearchClusterRunnerWrapper.class);
    private ElasticsearchClusterRunner runner;

    @Override
    public void startEmbeddedServer() {
        final ClassLoader currentClassLoader = this.getClass().getClassLoader();
        final ExecutorService executor = Executors.newSingleThreadExecutor();

        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.currentThread().setContextClassLoader(currentClassLoader);
                    runner = new ElasticsearchClusterRunner();
                    // create ES nodes
                    runner.onBuild(new ElasticsearchClusterRunner.Builder() {
                        @Override
                        public void build(int i, ImmutableSettings.Builder bldr) {
                            bldr.put("http.cors.enabled", true);
                        }
                    }).build(ElasticsearchClusterRunner.newConfigs()
                            //.clusterName("es-cl-run-" + System.currentTimeMillis())
                            .numOfNode(1)
                            .ramIndexStore());

                    runner.ensureYellow();
                    LOG.info("Embedded ElasticSearch started ...");
                } catch (Exception e) {
                    LOG.error("Error while starting ElasticSearch embedded server!", e);
                }
            }
        });

        try {
            LOG.info("Waiting for ElasticSearch embedded server...");
            executor.shutdown();
            executor.awaitTermination(20, TimeUnit.SECONDS);
            LOG.info("Finished waiting.");
        } catch (InterruptedException ex) {
            LOG.error("Error while waiting!", ex);
        }
    }

    @Override
    public void createIndex(String index, Map<String, Object> properties) {
        final ImmutableSettings.Builder builder = ImmutableSettings.builder();

        for (Map.Entry entry : properties.entrySet()) {
            builder.put(entry.getKey(), entry.getValue());
        }

        CreateIndexResponse createIndexResponse = runner.createIndex(index, builder.build());

        if (!createIndexResponse.isAcknowledged()) {
            throw new IllegalStateException("Index create response now acknowledged!");
        }
    }

    @Override
    public void stopEmbeddedServer() {
        runner.close();
        runner.clean();
    }
}
