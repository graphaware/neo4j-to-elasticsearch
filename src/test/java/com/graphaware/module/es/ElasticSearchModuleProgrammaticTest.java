/*
 * Copyright (c) 2013-2016 GraphAware
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

import com.graphaware.common.policy.inclusion.BaseNodeInclusionPolicy;
import com.graphaware.common.policy.inclusion.NodePropertyInclusionPolicy;
import com.graphaware.integration.es.test.EmbeddedElasticSearchServer;
import com.graphaware.integration.es.test.JestElasticSearchClient;
import com.graphaware.module.es.util.ServiceLoader;
import static com.graphaware.module.es.util.TestUtil.waitFor;
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
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.IOException;

import io.searchbox.core.DeleteByQuery;
import org.neo4j.graphdb.Transaction;

public class ElasticSearchModuleProgrammaticTest extends ElasticSearchModuleIntegrationTest {

    @Override
    public void setUp() {
        esServer = new EmbeddedElasticSearchServer();
        esServer.start();
        esClient = new JestElasticSearchClient(HOST, PORT);
    }

    @Test
    public void overallTest() throws IOException {
        dataShouldNotBeReplicatedWithModuleNotRegistered();
        cleanUpData();
        dataShouldBeCorrectlyReplicatedWithDefaultConfigEmptyDatabaseAndNoFailures();
        cleanUpData();
        dataShouldBeCorrectlyReplicatedWithCustomConfigEmptyDatabaseAndNoFailures();
        cleanUpData();
        dataShouldBeCorrectlyReplicatedWithPerRequestWriterAndNoFailures();
        cleanUpData();
        existingDatabaseShouldBeIndexedAndReIndexed();
        cleanUpData();
        dataShouldBeCorrectlyReplicatedWithRetryAfterFailureBulk();
        cleanUpData();
        dataShouldBeCorrectlyReplicatedWithRetryAfterFailurePerRequest();
        cleanUpData();
        dataShouldBeCorrectlyReplicatedWithRetryWhenEsStartsLate();
        cleanUpData();
    }

    //@Test
    public void dataShouldNotBeReplicatedWithModuleNotRegistered() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        writeSomeStuffToNeo4j();
        waitFor(200);
        verifyNoEsReplication();
        verifyEsEmpty();
    }

    //@Test
    public void dataShouldBeCorrectlyReplicatedWithDefaultConfigEmptyDatabaseAndNoFailures() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        //Framework & Modules setup:
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration(), database));

        configuration = ElasticSearchConfiguration.defaultConfiguration().withUri(HOST).withPort(PORT);
        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();

        //Actual test:
        writeSomeStuffToNeo4j();
        waitFor(3000);
        verifyEsReplication();
    }

    //@Test
    public void dataShouldBeCorrectlyReplicatedWithCustomConfigEmptyDatabaseAndNoFailures() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        //Framework & Modules setup:
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration().withUuidProperty("id"), database));

        configuration = ElasticSearchConfiguration.defaultConfiguration().withUri(HOST).withPort(PORT)
                .withMapping(ServiceLoader.loadMapping("DefaultMapping"), getDefaultMapping("different-index-name", "id"))
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
        waitFor(300);
        verifyEsReplication("different-index-name");
    }

    //@Test
    public void dataShouldBeCorrectlyReplicatedWithPerRequestWriterAndNoFailures() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        //Framework & Modules setup:
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration(), database));

        configuration = ElasticSearchConfiguration.defaultConfiguration().withUri(HOST).withPort(PORT).withExecuteBulk(false);
        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();

        //Actual test:
        writeSomeStuffToNeo4j();
        waitFor(400);
        verifyEsReplication();
    }

    //@Test
    public void existingDatabaseShouldBeIndexedAndReIndexed() throws IOException {
        TemporaryFolder folder = new TemporaryFolder();
        folder.create();
        folder.getRoot().deleteOnExit();

        //database.shutdown();
        database = new GraphDatabaseFactory().newEmbeddedDatabase(folder.getRoot());

        writeSomeStuffToNeo4j();

        //Framework & Modules setup:
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration(), database));

        configuration = ElasticSearchConfiguration.defaultConfiguration().withUri(HOST).withPort(PORT).withInitializeUntil(System.currentTimeMillis() + 1000);
        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();

        //Actual test - indexing:
        waitFor(400);
        verifyEsReplication();

        //stop DB
        database.shutdown();

        //Clear ES
        esServer.stop();
        esServer = new EmbeddedElasticSearchServer();
        esServer.start();

        //Restart DB with the plugins
        database = new GraphDatabaseFactory().newEmbeddedDatabase(folder.getRoot());
        verifyNoEsReplication();
        verifyEsEmpty();

        runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration(), database));

        configuration = ElasticSearchConfiguration.defaultConfiguration().withUri(HOST).withPort(PORT).withInitializeUntil(System.currentTimeMillis() + 1001);
        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();

        //Actual test - reindexing:
        waitFor(400);
        verifyEsReplication();
    }

//    @Test(timeout = 10_000)
//    @RepeatRule.Repeat(times = 5)
    public void dataShouldBeCorrectlyReplicatedWithRetryAfterFailureBulk() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        //Framework & Modules setup:
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration(), database));

        configuration = ElasticSearchConfiguration.defaultConfiguration().withUri(HOST).withPort(PORT).withRetryOnError(true);
        runtime.registerModule(new ElasticSearchModule("ES", new SometimesFailingElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();

        //Actual test:
        writeSomeStuffToNeo4j();
        verifyEventualEsReplication();
    }

//    @Test(timeout = 40_000)
//    @RepeatRule.Repeat(times = 5)
    public void dataShouldBeCorrectlyReplicatedWithRetryAfterFailurePerRequest() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        //Framework & Modules setup:
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration(), database));

        configuration = ElasticSearchConfiguration.defaultConfiguration().withUri(HOST).withPort(PORT).withRetryOnError(true).withExecuteBulk(false);
        runtime.registerModule(new ElasticSearchModule("ES", new SometimesFailingElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();

        //Actual test:
        writeSomeStuffToNeo4j();
        verifyEventualEsReplication();
    }

//    @Test(timeout = 20_000)
    public void dataShouldBeCorrectlyReplicatedWithRetryWhenEsStartsLate() {
        esServer.stop();
        esServer = new EmbeddedElasticSearchServer();

        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        //Framework & Modules setup:
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration(), database));

        configuration = ElasticSearchConfiguration.defaultConfiguration().withUri(HOST).withPort(PORT).withRetryOnError(true);
        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();

        //Actual test:
        writeSomeStuffToNeo4j();
        waitFor(200);

        esServer.start();
        verifyEventualEsReplication();
    }
    
    private void cleanUpData() {
        try (Transaction tx = database.beginTx()) {
            database.execute("MATCH ()-[r]-() DELETE r");
            database.execute("MATCH (p) DETACH DELETE p");
            tx.success();
        }
        
        DeleteByQuery delete = new DeleteByQuery.Builder("{ \"match_all\": {} }").addIndex("*")
            .addType("*")
            .build();
        esClient.execute(delete);
        database.shutdown();
        waitFor(1000);
    }
}
