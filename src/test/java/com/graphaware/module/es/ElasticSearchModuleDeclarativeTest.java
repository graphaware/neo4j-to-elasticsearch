/*
 * Copyright (c) 2013-2015 GraphAware
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

import com.graphaware.integration.es.test.EmbeddedElasticSearchServer;
import com.graphaware.integration.es.test.JestElasticSearchClient;
import com.graphaware.module.es.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.RepeatRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.IOException;

import static com.graphaware.runtime.RuntimeRegistry.getRuntime;
import static org.junit.Assert.assertEquals;


public class ElasticSearchModuleDeclarativeTest extends ElasticSearchModuleIntegrationTest {

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
    public void dataShouldNotBeReplicatedWithModuleNotRegistered() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        writeSomeStuffToNeo4j();
        TestUtil.waitFor(200);
        verifyNoEsReplication();
        verifyEsEmpty();
    }

    @Test
    public void dataShouldBeCorrectlyReplicatedWithDefaultConfigEmptyDatabaseAndNoFailures() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .loadPropertiesFromFile(properties("integration/int-test-default.properties"))
                .newGraphDatabase();

        getRuntime(database).waitUntilStarted();
        configuration = (ElasticSearchConfiguration) getRuntime(database).getModule("ES", ElasticSearchModule.class).getConfiguration();

        //Actual test:
        writeSomeStuffToNeo4j();
        TestUtil.waitFor(200);
        verifyEsReplication();
    }

    @Test
    public void dataShouldBeCorrectlyReplicatedWithCustomConfigEmptyDatabaseAndNoFailures() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .loadPropertiesFromFile(properties("integration/int-test-custom.properties"))
                .newGraphDatabase();

        getRuntime(database).waitUntilStarted();
        configuration = (ElasticSearchConfiguration) getRuntime(database).getModule("ES", ElasticSearchModule.class).getConfiguration();

        //Actual test:
        writeSomeStuffToNeo4j();
        TestUtil.waitFor(200);
        verifyEsReplication("different-index-name");
    }

    @Test
    public void dataShouldBeCorrectlyReplicatedWithPerRequestWriterAndNoFailures() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .loadPropertiesFromFile(properties("integration/int-test-write-per-request.properties"))
                .newGraphDatabase();

        getRuntime(database).waitUntilStarted();
        configuration = (ElasticSearchConfiguration) getRuntime(database).getModule("ES", ElasticSearchModule.class).getConfiguration();

        //Actual test:
        writeSomeStuffToNeo4j();
        TestUtil.waitFor(200);
        verifyEsReplication();
    }

    @Test(timeout = 20_000)
    public void existingDatabaseShouldBeIndexedAndReIndexed() throws IOException {
        TemporaryFolder folder = new TemporaryFolder();
        folder.create();
        folder.getRoot().deleteOnExit();

        database = new GraphDatabaseFactory().newEmbeddedDatabase(folder.getRoot().getAbsolutePath());

        writeSomeStuffToNeo4j();

        database.shutdown();

        database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(folder.getRoot().getAbsolutePath())
                .loadPropertiesFromFile(properties("integration/int-test-default-with-reindex.properties"))
                .newGraphDatabase();

        getRuntime(database).waitUntilStarted();
        configuration = (ElasticSearchConfiguration) getRuntime(database).getModule("ES", ElasticSearchModule.class).getConfiguration();

        TestUtil.waitFor(500);
        verifyEsReplication();

        database.shutdown();

        esServer.stop();
        esServer = new EmbeddedElasticSearchServer();
        esServer.start();

        database = new GraphDatabaseFactory().newEmbeddedDatabase(folder.getRoot().getAbsolutePath());

        verifyNoEsReplication();
        verifyEsEmpty();

        database.shutdown();

        database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(folder.getRoot().getAbsolutePath())
                .loadPropertiesFromFile(properties("integration/int-test-default-with-reindex2.properties")) //must be different than before so that re-indexing happens
                .newGraphDatabase();

        getRuntime(database).waitUntilStarted();
        configuration = (ElasticSearchConfiguration) getRuntime(database).getModule("ES", ElasticSearchModule.class).getConfiguration();

        verifyEventualEsReplication();
    }

    @Test(timeout = 10_000)
    @RepeatRule.Repeat(times = 5)
    public void dataShouldBeCorrectlyReplicatedWithRetryAfterFailureBulk() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .loadPropertiesFromFile(properties("integration/int-test-default-with-fail-and-retry.properties"))
                .newGraphDatabase();

        getRuntime(database).waitUntilStarted();
        configuration = (ElasticSearchConfiguration) getRuntime(database).getModule("ES", ElasticSearchModule.class).getConfiguration();

        writeSomeStuffToNeo4j();
        verifyEventualEsReplication();
    }

    @Test(timeout = 20_000)
    @RepeatRule.Repeat(times = 5)
    public void dataShouldBeCorrectlyReplicatedWithRetryAfterFailurePerRequest() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .loadPropertiesFromFile(properties("integration/int-test-default-with-fail-and-retry-per-request.properties"))
                .newGraphDatabase();

        getRuntime(database).waitUntilStarted();
        configuration = (ElasticSearchConfiguration) getRuntime(database).getModule("ES", ElasticSearchModule.class).getConfiguration();

        writeSomeStuffToNeo4j();
        verifyEventualEsReplication();
    }

    @Test(timeout = 20_000)
    public void dataShouldBeCorrectlyReplicatedWithRetryWhenEsStartsLate() {
        esServer.stop();
        esServer = new EmbeddedElasticSearchServer();

        database = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .loadPropertiesFromFile(properties("integration/int-test-default-with-retry.properties"))
                .newGraphDatabase();

        getRuntime(database).waitUntilStarted();
        configuration = (ElasticSearchConfiguration) getRuntime(database).getModule("ES", ElasticSearchModule.class).getConfiguration();

        //Actual test:
        writeSomeStuffToNeo4j();

        esServer.start();
        verifyEventualEsReplication();
    }

    @Test(timeout = 30_000)
    public void stressTest() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .loadPropertiesFromFile(properties("integration/int-test-stress.properties"))
                .newGraphDatabase();

        getRuntime(database).waitUntilStarted();
        configuration = (ElasticSearchConfiguration) getRuntime(database).getModule("ES", ElasticSearchModule.class).getConfiguration();

        int noNodes = 1000;
        Label[] labels = new Label[]{DynamicLabel.label("CAR"), DynamicLabel.label("CAR2")};

        try (Transaction tx = database.beginTx()) {
            for (int i = 0; i < noNodes; i++) {
                Node node = database.createNode(labels);
                node.setProperty("name", "Model_" + i);
                node.setProperty("manufacturer", "Tesla_ " + i);
            }
            tx.success();
        }

        verifyEventualEsReplication();
        assertEquals(labels.length * noNodes, new Neo4jElasticVerifier(database, configuration, esClient).countNodes());
    }

    private String properties(String name) {
        return this.getClass().getClassLoader().getResource(name).getPath();
    }
}
