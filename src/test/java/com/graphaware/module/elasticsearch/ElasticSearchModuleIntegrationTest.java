package com.graphaware.module.elasticsearch;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Get;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.IOException;

import static com.graphaware.runtime.RuntimeRegistry.getRuntime;
import static org.apache.commons.lang.Validate.isTrue;
import static org.apache.commons.lang3.Validate.notNull;

public class ElasticSearchModuleIntegrationTest {

    private static final String ES_HOST = "localhost";
    private static final String ES_PORT = "9200";
    private static final String ES_CONN = String.format("http://%s:%s", ES_HOST, ES_PORT);
    private static final String ES_INDEX = "cars";

    @Test
    public void test() {
        GraphDatabaseService database = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .loadPropertiesFromFile(this.getClass().getClassLoader().getResource("neo4j-elasticsearch.properties").getPath())
                .newGraphDatabase();

        getRuntime(database).waitUntilStarted();

        String nodeId;

        final Label car = DynamicLabel.label("CAR");

        try (Transaction tx = database.beginTx()) {
            Node node = database.createNode(car);
            node.setProperty("name", "Model S");
            node.setProperty("manufacturer", "Tesla");
            nodeId = String.valueOf(node.getId());
            tx.success();
        }

        JestClientFactory factory = new JestClientFactory();

        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(ES_CONN)
                .multiThreaded(true)
                .build());
        JestClient client = factory.getObject();

        Get get = new Get.Builder(ES_INDEX, nodeId).type(car.name()).build();
        JestResult result = null;

        try {
            result = client.execute(get);
        } catch (IOException e) {
            e.printStackTrace();
        }

        notNull(result);
        isTrue(result.isSucceeded());
    }
}
