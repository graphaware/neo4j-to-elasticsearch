/*
 * Copyright (c) 2015 GraphAware
 *
 * This file is part of GraphAware.
 *
 * GraphAware is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 */
package com.graphaware.module.es;

import com.google.gson.JsonObject;
import com.graphaware.common.policy.BaseNodeInclusionPolicy;
import com.graphaware.common.policy.NodePropertyInclusionPolicy;
import com.graphaware.integration.es.test.ElasticSearchClient;
import com.graphaware.integration.es.test.ElasticSearchServer;
import com.graphaware.integration.es.test.EmbeddedElasticSearchServer;
import com.graphaware.integration.es.test.JestElasticSearchClient;
import com.graphaware.module.uuid.UuidConfiguration;
import com.graphaware.module.uuid.UuidModule;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import io.searchbox.client.JestResult;
import io.searchbox.core.Count;
import io.searchbox.core.CountResult;
import io.searchbox.core.Get;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.RepeatRule;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.graphaware.elasticsearch.util.TestUtil.waitFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class ElasticSearchModuleEmbeddedProgrammaticTest {

    private static final String ES_INDEX = "neo4j-index";
    private static final String HOST = "localhost";
    private static final String PORT = "9201";
    private static final Label PERSON = DynamicLabel.label("Person");

    private GraphDatabaseService database;
    private ElasticSearchConfiguration configuration;
    private ElasticSearchServer esServer;
    private ElasticSearchClient esClient;

    @Before
    public void setUp() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
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
    public void dataShouldNotBeReplicatedWithModuleNotRegistered() {
        writeSomeStuffToNeo4j();
        waitFor(200);
        verifyNoEsReplication();
        verifyEsEmpty();
    }

    @Test
    public void dataShouldBeCorrectlyReplicatedWithDefaultConfigEmptyDatabaseAndNoFailures() {
        //Framework & Modules setup:
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration(), database));

        configuration = ElasticSearchConfiguration.defaultConfiguration(HOST, PORT);
        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();

        //Actual test:
        writeSomeStuffToNeo4j();
        waitFor(200);
        verifyEsReplication();
    }

    @Test
    public void dataShouldBeCorrectlyReplicatedWithCustomConfigEmptyDatabaseAndNoFailures() {
        //Framework & Modules setup:
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration().withUuidProperty("id"), database));

        configuration = ElasticSearchConfiguration.defaultConfiguration(HOST, PORT)
                .withIndexName("different-index-name")
                .withKeyProperty("id")
                .withQueueCapacity(1000)
                .withRetryOnError(false)
                .with(new BaseNodeInclusionPolicy() {
                    @Override
                    public boolean include(Node object) {
                        return object.hasLabel(PERSON);
                    }
                })
                .with(new NodePropertyInclusionPolicy() {
                    @Override
                    public boolean include(String key, Node propertyContainer) {
                        return !"age".equals(key);
                    }
                });
        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();

        //Actual test:
        writeSomeStuffToNeo4j();
        waitFor(200);
        verifyEsReplication("different-index-name");
    }

    @Test
    public void dataShouldBeCorrectlyReplicatedWithPerRequestWriterAndNoFailures() {
        //Framework & Modules setup:
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration(), database));

        configuration = ElasticSearchConfiguration.defaultConfiguration(HOST, PORT).withExecuteBulk(false);
        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();

        //Actual test:
        writeSomeStuffToNeo4j();
        waitFor(200);
        verifyEsReplication();
    }

    @Test
    public void existingDatabaseShouldBeIndexedAndReIndexed() throws IOException {
        TemporaryFolder folder = new TemporaryFolder();
        folder.create();
        folder.getRoot().deleteOnExit();

        database.shutdown();
        database = new GraphDatabaseFactory().newEmbeddedDatabase(folder.getRoot().getAbsolutePath());

        writeSomeStuffToNeo4j();

        //Framework & Modules setup:
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration(), database));

        configuration = ElasticSearchConfiguration.defaultConfiguration(HOST, PORT).withReindexUntil(System.currentTimeMillis() + 1000);
        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();

        //Actual test - indexing:
        waitFor(200);
        verifyEsReplication();

        //stop DB
        database.shutdown();

        //Clear ES
        esServer.stop();
        esServer = new EmbeddedElasticSearchServer();
        esServer.start();

        //Restart DB with the plugins
        database = new GraphDatabaseFactory().newEmbeddedDatabase(folder.getRoot().getAbsolutePath());
        verifyNoEsReplication();
        verifyEsEmpty();

        runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration(), database));

        configuration = ElasticSearchConfiguration.defaultConfiguration(HOST, PORT).withReindexUntil(System.currentTimeMillis() + 1000);
        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();

        //Actual test - reindexing:
        waitFor(200);
        verifyEsReplication();
    }

    @Test
    @RepeatRule.Repeat(times = 5)
    public void dataShouldBeCorrectlyReplicatedWithRetryAfterFailureBulk() {
        //Framework & Modules setup:
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration(), database));

        configuration = ElasticSearchConfiguration.defaultConfiguration(HOST, PORT).withRetryOnError(true);
        runtime.registerModule(new ElasticSearchModule("ES", new SometimesFailingElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();

        //Actual test:
        writeSomeStuffToNeo4j();
        waitFor(500);
        verifyEsReplication();
    }

    @Test
    @RepeatRule.Repeat(times = 5)
    public void dataShouldBeCorrectlyReplicatedWithRetryAfterFailurePerRequest() {
        //Framework & Modules setup:
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration(), database));

        configuration = ElasticSearchConfiguration.defaultConfiguration(HOST, PORT).withRetryOnError(true).withExecuteBulk(false);
        runtime.registerModule(new ElasticSearchModule("ES", new SometimesFailingElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();

        //Actual test:
        writeSomeStuffToNeo4j();
        waitFor(500);
        verifyEsReplication();
    }

    @Test
    public void dataShouldBeCorrectlyReplicatedWithRetryWhenEsStartsLate() {
        esServer.stop();
        esServer = new EmbeddedElasticSearchServer();

        //Framework & Modules setup:
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration(), database));

        configuration = ElasticSearchConfiguration.defaultConfiguration(HOST, PORT).withRetryOnError(true);
        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();

        //Actual test:
        writeSomeStuffToNeo4j();
        waitFor(200);

        esServer.start();
        waitFor(1000);
        verifyEsReplication();
    }

    private void writeSomeStuffToNeo4j() {
        //tx1
        database.execute("CREATE (p:Person {name:'Michal', age:30})-[:WORKS_FOR {since:2013, role:'MD'}]->(c:Company {name:'GraphAware', est: 2013})");

        //tx2
        database.execute("MATCH (ga:Company {name:'GraphAware'}) CREATE (p:Person {name:'Adam'})-[:WORKS_FOR {since:2014}]->(ga)");

        //tx3
        try (Transaction tx = database.beginTx()) {
            database.execute("MATCH (ga:Company {name:'GraphAware'}) CREATE (p:Person {name:'Daniela'})-[:WORKS_FOR]->(ga)");
            database.execute("MATCH (p:Person {name:'Michal'}) SET p.age=31");
            database.execute("MATCH (p:Person {name:'Adam'})-[r]-() DELETE p,r");
            database.execute("MATCH (p:Person {name:'Michal'})-[r:WORKS_FOR]->() REMOVE r.role");
            tx.success();
        }
    }

    private void verifyEsReplication() {
        verifyEsReplication(ES_INDEX);
    }

    private void verifyEsReplication(String index) {
        try (Transaction tx = database.beginTx()) {
            for (Node node : GlobalGraphOperations.at(database).getAllNodes()) {
                if (configuration.getInclusionPolicies().getNodeInclusionPolicy().include(node)) {
                    verifyEsReplication(node, index);
                }
            }
            tx.success();
        }
    }

    private void verifyNoEsReplication() {
        try (Transaction tx = database.beginTx()) {
            for (Node node : GlobalGraphOperations.at(database).getAllNodes()) {
                verifyNoEsReplication(node);
            }
            tx.success();
        }
    }

    private void verifyEsReplication(Node node) {
        verifyEsReplication(node, ES_INDEX);
    }

    private void verifyEsReplication(Node node, String index) {
        Map<String, Object> properties = new HashMap<>();
        Set<String> labels = new HashSet<>();
        String nodeKey;
        try (Transaction tx = database.beginTx()) {
            nodeKey = node.getProperty(configuration.getKeyProperty()).toString();

            for (String key : node.getPropertyKeys()) {
                if (configuration.getInclusionPolicies().getNodePropertyInclusionPolicy().include(key, node)) {
                    properties.put(key, node.getProperty(key));
                }
            }

            for (Label label : node.getLabels()) {
                labels.add(label.name());
            }

            tx.success();
        }

        for (String label : labels) {
            Get get = new Get.Builder(index, nodeKey).type(label).build();
            JestResult result = esClient.execute(get);

            assertTrue(result.getErrorMessage(), result.isSucceeded());
            assertEquals(configuration.getIndex(), result.getValue("_index"));
            assertEquals(label, result.getValue("_type"));
            assertEquals(nodeKey, result.getValue("_id"));
            assertTrue((Boolean) result.getValue("found"));

            JsonObject source = result.getJsonObject().getAsJsonObject("_source");
            assertEquals(properties.size(), source.entrySet().size());
            for (String key : properties.keySet()) {
                assertEquals(properties.get(key).toString(), source.get(key).getAsString());
            }
        }
    }

    private void verifyNoEsReplication(Node node) {
        Set<String> labels = new HashSet<>();
        String nodeKey;
        try (Transaction tx = database.beginTx()) {
            if (configuration != null) {
                nodeKey = node.getProperty(configuration.getKeyProperty()).toString();
            } else {
                nodeKey = node.getProperty("uuid", "unknown").toString();
            }

            for (Label label : node.getLabels()) {
                labels.add(label.name());
            }

            tx.success();
        }

        for (String label : labels) {
            Get get = new Get.Builder(ES_INDEX, nodeKey).type(label).build();
            JestResult result = esClient.execute(get);

            assertTrue(!result.isSucceeded() || !((Boolean) result.getValue("found")));
        }
    }

    //todo this method never fails, claims 0 results even when there are things in the index. Alessandro pls help
    private void verifyEsEmpty() {
        Count.Builder count = new Count.Builder().query("");
        count = count.addIndex(ES_INDEX);

        try (Transaction tx = database.beginTx()) {
            for (Label label : GlobalGraphOperations.at(database).getAllLabels()) {
                count = count.addType(label.name());
                tx.success();
            }
        }

        CountResult result = esClient.execute(count.build());

        if (result.isSucceeded()) {
            assertEquals(0.0, result.getCount(), 0.001);
        }
    }

    private Node findNode(Label label, String property, String value) {
        Node result;
        try (Transaction tx = database.beginTx()) {
            result = database.findNode(label, property, value);
            tx.success();
        }
        return result;
    }
}
