package com.graphaware.module.es;


import com.graphaware.common.policy.inclusion.all.IncludeAllRelationships;
import com.graphaware.integration.es.test.EmbeddedElasticSearchServer;
import com.graphaware.integration.es.test.JestElasticSearchClient;
import com.graphaware.module.es.mapping.JsonFileMapping;
import com.graphaware.module.es.util.ServiceLoader;
import com.graphaware.module.uuid.UuidConfiguration;
import com.graphaware.module.uuid.UuidModule;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class BulkIndexOnInitializeIntegrationTest extends ElasticSearchModuleIntegrationTest {

    @Test
    public void testBulkInsertsDuringInitializePhaseAreBatchedCorrectly() throws Exception {
        IntStream.range(0, 10).forEach(i -> {
            try (Transaction tx = database.beginTx()) {
                database.execute("UNWIND range(0, 1000) AS i CREATE (n:Person {id: i, firstName: 'node' + i, lastName: 'node' + i})");
                tx.success();
            }
        });

        IntStream.range(0, 5).forEach(i -> {
            try (Transaction tx = database.beginTx()) {
                database.execute("UNWIND range(0, 1000) AS i CREATE (n:Entity {id: i, firstName: 'node' + i, lastName: 'node' + i})");
                tx.success();
            }
        });

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration().withInitializeUntil(System.currentTimeMillis()+100000), database));

        JsonFileMapping mapping = (JsonFileMapping) ServiceLoader.loadMapping("com.graphaware.module.es.mapping.JsonFileMapping");
        Map<String, String> mappingConfig = new HashMap<>();
        mappingConfig.put("file", "integration/mapping-basic.json");

        configuration = ElasticSearchConfiguration.defaultConfiguration()
                .withInitializeUntil(System.currentTimeMillis() + 10000)
                .withMapping(mapping, mappingConfig)
                .withExecuteBulk(true)
                .withReindexBatchSize(1000)
                .withQueueCapacity(10000)
                .with(IncludeAllRelationships.getInstance())
                .withUri(HOST)
                .withPort(PORT);

        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));
        runtime.start();
        runtime.waitUntilStarted();


        boolean hasUuidSet = false;
        try (Transaction tx = database.beginTx()) {
            Result result = database.execute("MATCH (n:Person) RETURN n LIMIT 1");
            while (result.hasNext()) {
                Node node = (Node) result.next().get("n");
                hasUuidSet = node.hasProperty("uuid");
            }
            tx.success();
        }

        assertTrue(hasUuidSet);

        runtime.getModule(ElasticSearchModule.class).reindexNodes(database);

    }
}
