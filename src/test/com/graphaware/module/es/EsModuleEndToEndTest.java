package com.graphaware.module.es;

import com.graphaware.test.integration.NeoServerIntegrationTest;
import org.eclipse.jetty.http.HttpStatus;
import org.elasticsearch.node.Node;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.junit.Assert.assertEquals;

public class EsModuleEndToEndTest extends NeoServerIntegrationTest {

    private static String ELASTICSEARCH_URL = "http://localhost:9200";
    private static Node ELASTICSEARCH_NODE;

    @Override
    protected String neo4jConfigFile() {
        return "neo4j-es.properties";
    }

    @Override
    public void setUp() throws IOException, InterruptedException {
        super.setUp();
        ELASTICSEARCH_NODE = nodeBuilder().node();
        ELASTICSEARCH_NODE.start();
    }

    @Override
    public void tearDown() throws IOException, InterruptedException {
        super.tearDown();

        ELASTICSEARCH_NODE.close();
    }

    @Test
    public void testIntegration() {
        httpClient.executeCypher(baseUrl(), "CREATE (c:Car {name:'Tesla Model S'})");

        String response = httpClient.get(ELASTICSEARCH_URL + "/_cluster/health?pretty=true", HttpStatus.OK_200);

        assertEquals("", response);
    }
}