package com.graphaware.integration.elasticsearch;

import com.graphaware.runtime.module.BaseTxDrivenModule;
import com.graphaware.runtime.module.DeliberateTransactionRollbackException;
import com.graphaware.tx.event.improved.api.ImprovedTransactionData;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class ElasticSearchModule extends BaseTxDrivenModule<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchBootstrapper.class);

    private final ElasticSearchConfiguration config;

    public ElasticSearchModule(String moduleId, ElasticSearchConfiguration config, GraphDatabaseService database) {
        super(moduleId);
        this.config = config;
    }

    @Override
    public Void beforeCommit(ImprovedTransactionData transactionData) throws DeliberateTransactionRollbackException {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(String.format("http://%s:%s", config.getElasticSearchUri(), config.getElasticSearchPort()))
                .multiThreaded(true)
                .build());
        JestClient client = factory.getObject();


        try {
            final JestResult execute = client.execute(new CreateIndex.Builder(config.getElasticSearchIndex()).build());
            LOG.info(execute.getErrorMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (Node node : transactionData.getAllCreatedNodes()) {
            String label = node.getLabels().iterator().next().name().toString();
            String id = String.valueOf(node.getId());

            Map<String, String> source = new LinkedHashMap<>();

            for (String key : node.getPropertyKeys()) {
                source.put(key, String.valueOf(node.getProperty(key)));
            }

            Index index = new Index.Builder(source).index(config.getElasticSearchIndex()).type(label).id(id).build();

            try {
                final JestResult execute = client.execute(index);
                LOG.debug(execute.getErrorMessage());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return null;
    }
}
