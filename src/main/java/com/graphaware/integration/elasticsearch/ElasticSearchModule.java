package com.graphaware.integration.elasticsearch;

import com.graphaware.runtime.module.BaseTxDrivenModule;
import com.graphaware.runtime.module.DeliberateTransactionRollbackException;
import com.graphaware.tx.event.improved.api.Change;
import com.graphaware.tx.event.improved.api.ImprovedTransactionData;
import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class ElasticSearchModule extends BaseTxDrivenModule<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchModuleBootstrapper.class);

    private final ElasticSearchConfiguration config;

    private JestClient client;

    public ElasticSearchModule(String moduleId, ElasticSearchConfiguration config, GraphDatabaseService database) {
        super(moduleId);
        this.config = config;
        createIndexIfNotExist();
    }

    private void createIndexIfNotExist() {
        try {
            boolean indixesExist = getClient().execute(new IndicesExists.Builder(config.getElasticSearchIndex()).build()).isSucceeded();
            if (indixesExist)
                return;
            final JestResult execute = getClient().execute(new CreateIndex.Builder(config.getElasticSearchIndex()).build());
            LOG.info(execute.getErrorMessage());
        } catch (IOException e) {
            LOG.error("Error while creating indices " + config.getElasticSearchIndex(), e);
            if (config.isMandatory())
                throw new RuntimeException("Error while creating indices " + config.getElasticSearchIndex(), e);
        }
    }

    private JestClient getClient() {
        if (client != null)
            return client;

        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder(String.format("http://%s:%s", config.getElasticSearchUri(), config.getElasticSearchPort()))
                .multiThreaded(true)
                .build());
        client = factory.getObject();
        return client;
    }

    @Override
    public Void beforeCommit(ImprovedTransactionData transactionData) throws DeliberateTransactionRollbackException {
        for (Node node : transactionData.getAllCreatedNodes())
            createNodeInES(node);

        for (Node node : transactionData.getAllDeletedNodes())
            deleteNodeFromES(node);

        for (Change<Node> change : transactionData.getAllChangedNodes())
            updateNodeInES(change.getCurrent());

        return null;
    }

    private void createNodeInES(Node node) throws RuntimeException {
        List<String> labels = new ArrayList<>();
        final Iterator<Label> labelsIterator = node.getLabels().iterator();
        while (labelsIterator != null && labelsIterator.hasNext())
            labels.add(labelsIterator.next().name());
        String id = String.valueOf(node.getId());
        Map<String, String> source = new LinkedHashMap<>();
        for (String key : node.getPropertyKeys())
            source.put(key, String.valueOf(node.getProperty(key)));
        for (String label : labels) {
            insertNode(source, label, id);
        }
    }

    private void deleteNodeFromES(Node node) {
        List<String> labels = new ArrayList<>();
        final Iterator<Label> labelsIterator = node.getLabels().iterator();
        while (labelsIterator != null && labelsIterator.hasNext())
            labels.add(labelsIterator.next().name());
        String id = String.valueOf(node.getId());
        for (String label : labels) {
            Delete delete = new Delete.Builder(id).index(config.getElasticSearchIndex()).type(label).build();
            execute(delete, id);
        }
    }

    private void updateNodeInES(Node node) {
        List<String> labels = new ArrayList<>();
        final Iterator<Label> labelsIterator = node.getLabels().iterator();
        while (labelsIterator != null && labelsIterator.hasNext())
            labels.add(labelsIterator.next().name());
        String id = String.valueOf(node.getId());
        Map<String, String> source = new LinkedHashMap<>();
        for (String key : node.getPropertyKeys())
            source.put(key, String.valueOf(node.getProperty(key)));
        for (String label : labels) {
            insertNode(source, label, id);
        }
    }

    private void insertNode(Object source, String label, String id) {
        Index index = new Index.Builder(source).index(config.getElasticSearchIndex()).type(label).id(id).build();
        execute(index, id);
    }

    private void execute(Action<? extends JestResult> insert, String nodeId) {
        try {
            final JestResult execute = client.execute(insert);
            LOG.debug(execute.getErrorMessage());
        } catch (IOException e) {
            String errorDescription = "Error while inserting node " + nodeId;
            LOG.error(errorDescription, e);
            if (config.isMandatory())
                throw new RuntimeException(errorDescription, e);
        }
    }
}
