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
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.HashMap;
import java.util.Map;


public class ElasticSearchModuleAdvancedBaseMappingTest extends ElasticSearchModuleIntegrationTest {

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
    public void testArray() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration(), database));

        Mapping mapping = ServiceLoader.loadMapping("com.graphaware.module.es.mapping.AdvancedMapping");
        Map<String, String> config = new HashMap<>();
        mapping.configure(config);

        configuration = ElasticSearchConfiguration.defaultConfiguration()
                .withMapping(mapping)
                .withUri(HOST)
                .withPort(PORT);
        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();

        writeSomeStuffWithListToNeo4j();
        TestUtil.waitFor(500);
        verifyEsAdvancedReplication();
    }

    protected void writeSomeStuffWithArrayToNeo4j() {
        //tx1
        database.execute("CREATE (p:Person {name:'Michal', age:30})-[:WORKS_FOR {since:2013, role:'MD'}]->(c:Company {name:'GraphAware', est: 2013, roles:[\"CEO\", \"CTO\"]})");
    }
    
    protected void writeSomeStuffWithListToNeo4j() {
        //tx2
        try (Transaction tx = database.beginTx()) {
            Node node = database.createNode(Label.label("LabelWithList"));
            node.addLabel(Label.label("LabelWithList1"));
            node.addLabel(Label.label("LabelWithList2"));
            int[] listOfInteger = {1, 2, 3};
            node.setProperty("listOfInteger", listOfInteger);
            tx.success();
        }
        
    }
}
