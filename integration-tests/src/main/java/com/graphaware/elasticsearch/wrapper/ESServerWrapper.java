/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.graphaware.elasticsearch.wrapper;

import com.esotericsoftware.minlog.Log;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author ale
 */
public class ESServerWrapper implements IGenericServerWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(ESServerWrapper.class);
    private ElasticsearchClusterRunner runner;

    public ESServerWrapper() {

    }

    public void startEmbdeddedServer() {
        final ClassLoader currentClassLoader = this.getClass().getClassLoader();
        final CountDownLatch done = new CountDownLatch(1);
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
                    Log.warn("Embedded ElasticSearch started ...");
                    done.countDown();
                } catch (Exception e) {
                    LOG.error("Error while starting ES embedded server up ...", e);
                }
            }
        });
        try {
            Log.warn("Waiting for embedded startup completion ...");
            done.await(20, TimeUnit.SECONDS);
            Log.warn("... time is up!");
        } catch (InterruptedException ex) {
            Log.error("Error while waiting");
        }

    }

    @Override
    public void createIndex(String index, Map<String, Object> properties) {
        final ImmutableSettings.Builder builder = ImmutableSettings
                .builder();
        for (Map.Entry entry : properties.entrySet()) {
            builder.put(entry.getKey(), entry.getValue());
        }

        CreateIndexResponse createIndexResponse = runner
                .createIndex(index, builder.build());

        assert createIndexResponse.isAcknowledged();
    }

    @Override
    public void stopEmbdeddedServer() {
        runner.close();
        runner.clean();
    }
}
