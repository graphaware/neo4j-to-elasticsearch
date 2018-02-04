/*
 * Copyright (c) 2015-2017 GraphAware
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
package graphaware.module.es;

import com.graphaware.common.policy.BaseNodeInclusionPolicy;
import com.graphaware.common.policy.NodePropertyInclusionPolicy;
import com.graphaware.integration.es.test.EmbeddedElasticSearchServer;
import com.graphaware.integration.es.test.JestElasticSearchClient;
import com.graphaware.module.uuid.UuidConfiguration;
import com.graphaware.module.uuid.UuidModule;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.RepeatRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.IOException;

import static graphaware.elasticsearch.util.TestUtil.waitFor;

public class ElasticSearchModuleProgrammaticTest extends ElasticSearchModuleIntegrationTest {

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
        waitFor(1000);
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
                    public boolean include(String key, Node entity) {
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

    @Test(timeout = 10_000)
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
        verifyEventualEsReplication();
    }

    @Test(timeout = 10_000)
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
        verifyEventualEsReplication();
    }

    @Test(timeout = 10_000)
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
        verifyEventualEsReplication();
    }
}
