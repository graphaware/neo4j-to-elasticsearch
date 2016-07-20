package com.graphaware.module.es;

import com.graphaware.integration.es.test.EmbeddedElasticSearchServer;
import com.graphaware.integration.es.test.JestElasticSearchClient;
import com.graphaware.module.es.mapping.Mapping;
import com.graphaware.module.es.util.ServiceLoader;
import com.graphaware.module.es.util.TestUtil;
import com.graphaware.module.uuid.UuidConfiguration;
import com.graphaware.module.uuid.UuidModule;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ElasticSearchModuleJsonMappingTest extends ElasticSearchModuleIntegrationTest {

    @Before
    public void setUp() {
        esServer = new EmbeddedElasticSearchServer();
        esServer.start();
        esClient = new JestElasticSearchClient(HOST, PORT);

    }

    @After
    public void tearDown() {
        database.shutdown();
        esServer.stop();
        esClient.shutdown();
    }

    @Test
    public void testBasicJsonMappingModuleBootstrap() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration(), database));

        Mapping mapping = ServiceLoader.loadMapping("com.graphaware.module.es.mapping.JsonFileMapping");
        Map<String, String> config = new HashMap<>();
        config.put("file", "integration/mapping-basic.json");
        mapping.configure(config);
        System.out.println(mapping.getKeyProperty());

        configuration = ElasticSearchConfiguration.defaultConfiguration()
                .withMapping(mapping)
                .withUri(HOST)
                .withPort(PORT);

        assertEquals("uuid", configuration.getMapping().getKeyProperty());
    }

}
