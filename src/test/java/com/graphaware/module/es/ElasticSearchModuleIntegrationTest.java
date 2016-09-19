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

import com.graphaware.integration.es.test.ElasticSearchClient;
import com.graphaware.integration.es.test.ElasticSearchServer;
import com.graphaware.integration.es.test.EmbeddedElasticSearchServer;
import com.graphaware.integration.es.test.JestElasticSearchClient;
import org.junit.After;
import org.junit.Before;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.graphaware.module.es.util.TestUtil.waitFor;


public abstract class ElasticSearchModuleIntegrationTest {

    protected static final String HOST = "localhost";
    protected static final String PORT = "9201";
    protected static final Label PERSON = Label.label("Person");

    protected GraphDatabaseService database;
    protected ElasticSearchConfiguration configuration;
    protected ElasticSearchServer esServer;
    protected ElasticSearchClient esClient;

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

    protected void writeSomeStuffToNeo4j() {
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

    protected void verifyEventualEsReplication() {
        Neo4jElasticVerifier verifier = new Neo4jElasticVerifier(database, configuration, esClient);

        while (true) {
            try {
                verifier.verifyEsReplication();
                return;
            } catch (AssertionError e) {
                //not yet
                waitFor(100);
            }
        }
    }

    protected void verifyEsReplication() {
        new Neo4jElasticVerifier(database, configuration, esClient).verifyEsReplication();
    }
    
    protected void verifyEsAdvancedReplication() {
        new Neo4jElasticVerifier(database, configuration, esClient).verifyEsAdvancedReplication();
    }

    protected void verifyEsReplication(String indexprefix) {
        new Neo4jElasticVerifier(database, configuration, esClient).verifyEsReplication(indexprefix);
    }

    protected void verifyNoEsReplication() {
        new Neo4jElasticVerifier(database, configuration, esClient).verifyNoEsReplication();
    }

    protected void verifyEsReplication(Node node) {
        new Neo4jElasticVerifier(database, configuration, esClient).verifyEsReplication(node);
    }

    protected void verifyEsReplication(Node node, String indexPrefix) {
        new Neo4jElasticVerifier(database, configuration, esClient).verifyEsReplication(node, indexPrefix);
    }

    protected void verifyNoEsReplication(Node node) {
        new Neo4jElasticVerifier(database, configuration, esClient).verifyNoEsReplication(node);
    }

    protected void verifyEsEmpty() {
        new Neo4jElasticVerifier(database, configuration, esClient).verifyEsEmpty();
    }

    public Node findNode(Label label, String property, String value) {
        Node result;
        try (Transaction tx = database.beginTx()) {
            result = database.findNode(label, property, value);
            tx.success();
        }
        return result;
    }

    protected Map<String, String> getDefaultMapping(String index, String keyProperty) {
        Map<String, String> params = new HashMap<>();
        params.put("index", index);
        params.put("keyProperty", keyProperty);
        return params;
    }

    protected List<String> labelsToStrings(Node node) {
        List<String> list = new ArrayList<>();
        try (Transaction tx = database.beginTx()) {
            for (Label l : node.getLabels()) {
                list.add(l.name());
            }

            tx.success();
        }

        return list;
    }
}
