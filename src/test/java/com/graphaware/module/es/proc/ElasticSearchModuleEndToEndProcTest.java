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

package com.graphaware.module.es.proc;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.ResourceIterator;

import java.util.List;
import java.util.Map;

import static com.graphaware.module.es.util.TestUtil.waitFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ElasticSearchModuleEndToEndProcTest extends ESProcedureIntegrationTest {

    protected static final int WAIT_TIME = 1500;

    @Test
    public void totalTest() {
        testNodeWorkflow();
        cleanUpData();
        testQueryNodeRawWorkflow();
        cleanUpData();
        testRelationshipWorkflow();
        cleanUpData();
        testQueryRelationshipRawWorkflow();
        cleanUpData();
        testIsReindexedProcedure();
        cleanUpData();
    }
    
    //@Test
    public void testNodeWorkflow() {
        writeSomeStuffToNeo4j();
        waitFor(WAIT_TIME);

        // match all nodes
        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.es.queryNode('{\"query\":{\"match_all\":{}}}') YIELD node return node");
            ResourceIterator<Node> resIterator = result.columnAs("node");
            assertEquals(4, resIterator.stream().count());

            tx.success();
        }

        // match 2 nodes
        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.es.queryNode('{\"query\":{\"match\":{\"name\":\"michal\"}}}') YIELD node, score return node, score");
            List<String> columns = result.columns();
            assertEquals(2, columns.size());

            int count = 0;
            while (result.hasNext()) {
                count++;
                Map<String, Object> next = result.next();
                assertTrue(next.get("node") instanceof Node);
                assertTrue(next.get("score") instanceof Double);
                assertTrue(((String) ((Node) next.get("node")).getProperty("name")).contains("Michal"));
            }
            assertEquals(2, count);
            tx.success();
        }

        // match no node
        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.es.queryNode('{\"query\":{\"match\":{\"name\":\"alessandro\"}}}') YIELD node, score return node, score");
            ResourceIterator<Node> resIterator = result.columnAs("node");
            assertEquals(0, resIterator.stream().count());
            tx.success();
        }
    }

    //@Test
    public void testQueryNodeRawWorkflow() {
        writeSomeStuffToNeo4j();
        waitFor(WAIT_TIME);

        // count all nodes
        try(Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.es.queryNodeRaw('{\"query\":{\"match_all\":{}}, \"size\":0}') YIELD json return json");
            ResourceIterator<String> resIterator = result.columnAs("json");

            JsonElement e = new JsonParser().parse(resIterator.next());
            JsonObject hits = e.getAsJsonObject().get("hits").getAsJsonObject();
            assertEquals(4, hits.get("total").getAsInt());
            assertEquals(0, hits.get("hits").getAsJsonArray().size());

            tx.success();
        }
    }

    //@Test
    public void testRelationshipWorkflow() {
        writeSomeStuffToNeo4j();
        waitFor(WAIT_TIME);

        // match all relationships
        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.es.queryRelationship('{\"query\":{\"match_all\":{}}}') YIELD relationship return relationship");
            ResourceIterator<Relationship> resIterator = result.columnAs("relationship");
            assertEquals(3, resIterator.stream().count());

            tx.success();
        }

        // match 1 relationship
        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.es.queryRelationship('{\"query\":{\"match\":{\"since\":\"2014\"}}}') YIELD relationship, score return relationship, score");
            List<String> columns = result.columns();
            assertEquals(2, columns.size());

            int count = 0;
            while (result.hasNext()) {
                count++;
                Map<String, Object> next = result.next();
                assertTrue(next.get("relationship") instanceof Relationship);
                assertTrue(next.get("score") instanceof Double);
                assertEquals(((Relationship) next.get("relationship")).getProperty("since"), 2014L);
            }
            assertEquals(1, count);
            tx.success();
        }

        // match no relationship
        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.es.queryRelationship('{\"query\":{\"match\":{\"since\":\"1942\"}}}') YIELD relationship, score return relationship, score");
            ResourceIterator<Node> resIterator = result.columnAs("relationship");
            assertEquals(0, resIterator.stream().count());
            tx.success();
        }
    }

    //@Test
    public void testQueryRelationshipRawWorkflow() {
        writeSomeStuffToNeo4j();
        waitFor(WAIT_TIME);

        // count all nodes
        try(Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.es.queryRelationshipRaw('{\"query\":{\"match_all\":{}}, \"size\":0}') YIELD json return json");
            ResourceIterator<String> resIterator = result.columnAs("json");

            JsonElement e = new JsonParser().parse(resIterator.next());
            JsonObject hits = e.getAsJsonObject().get("hits").getAsJsonObject();
            assertEquals(3, hits.get("total").getAsInt());
            assertEquals(0, hits.get("hits").getAsJsonArray().size());

            tx.success();
        }
    }

    //@Test
    public void testIsReindexedProcedure() {
        boolean resultFound = false;
        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.es.initialized() YIELD status RETURN status");
            while (result.hasNext()) {
                Map<String, Object> record = result.next();
                assertTrue((boolean) record.get("status"));
                resultFound = true;
            }

            tx.success();
        }
        assertTrue(resultFound);
    }

    private String writeSomeStuffToNeo4j() {
        //tx1
        httpClient.executeCypher(baseNeoUrl(), "CREATE (p:Person {name:'Michal Bachman', age:30})-[:WORKS_FOR {since:2013, role:'MD'}]->(c:Company {name:'GraphAware', est: 2013})");
        
        //tx2
        httpClient.executeCypher(baseNeoUrl(), "MATCH (ga:Company {name:'GraphAware'}) CREATE (p:Person {name:'Adam'})-[:WORKS_FOR {since:2014}]->(ga)");
        
        //tx3
        httpClient.executeCypher(baseNeoUrl(), "MATCH (ga:Company {name:'GraphAware'}) CREATE (p:Person {name:'Michal Teck', age:33})-[:WORKS_FOR {since:2014, role:'Senior Consultant'}]->(ga)");


        //tx4
        httpClient.executeCypher(baseNeoUrl(),
                "MATCH (ga:Company {name:'GraphAware'}) CREATE (p:Person {name:'Daniela'})-[:WORKS_FOR]->(ga)",
                "MATCH (p:Person {name:'Michal'}) SET p.age=31",
                "MATCH (p:Person {name:'Adam'})-[r]-() DELETE p,r",
                "MATCH (p:Person {name:'Michal'})-[r:WORKS_FOR]->() REMOVE r.role");

        return httpClient.executeCypher(baseNeoUrl(), "MATCH (p:Person {name:'Michal'}) RETURN p.uuid").replace("{\"results\":[{\"columns\":[\"p.uuid\"],\"data\":[{\"row\":[\"", "").replace("\"],\"meta\":[null]}]}],\"errors\":[]}", "");
    }

    private void cleanUpData() {
        httpClient.executeCypher(baseNeoUrl(), "MATCH (p) DETACH DELETE p");
        waitFor(WAIT_TIME);
    }
}
