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

import com.graphaware.integration.es.test.ElasticSearchServer;
import com.graphaware.integration.es.test.EmbeddedElasticSearchServer;
import com.graphaware.test.integration.GraphAwareIntegrationTest;
import org.json.JSONException;
import org.junit.Test;

import static com.graphaware.module.es.util.TestUtil.waitFor;
import java.util.List;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public class ElasticSearchModuleEndToEndProcTest extends GraphAwareIntegrationTest {

    protected ElasticSearchServer esServer;

    @Override
    protected String configFile() {
        return "integration/int-test-default.conf";
    }

    @Override
    public void setUp() throws Exception {
        esServer = new EmbeddedElasticSearchServer();
        esServer.start();
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        esServer.stop();
        super.tearDown();
    }

    @Test
    public void testWorkflow() throws JSONException {
        String uuid = writeSomeStuffToNeo4j();
        waitFor(2000);
        try( Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.es.query({query: '{\"query\":{\"match_all\":{}}}'}) YIELD node return node");
            ResourceIterator<Node> resIterator = result.columnAs("node");
            assertEquals(4, resIterator.stream().count());
            tx.success();
        }
        
        try( Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.es.query({query: '{\"query\":{\"match\":{\"name\":\"michal\"}}}'}) YIELD node, score return node, score");
            List<String> columns = result.columns();
            assertEquals(2, columns.size());
            
            int count = 0;
            while (result.hasNext()) {
                count++;
                Map<String, Object> next = result.next();
                assertTrue(next.get("node") instanceof Node);
                assertTrue(next.get("score") instanceof Float);
            }
            assertEquals(2, count);
            tx.success();
        }
        
        try( Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.es.query({query: '{\"query\":{\"match\":{\"name\":\"alessandro\"}}}'}) YIELD node, score return node, score");
            ResourceIterator<Node> resIterator = result.columnAs("node");
            assertEquals(0, resIterator.stream().count());
            tx.success();
        }
    }

    @Test
    public void testIsReindexedProcedure() {
        boolean resultFound = false;
        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.es.initialized() YIELD status RETURN status");
            while (result.hasNext()) {
                Map<String, Object> record = result.next();
                System.out.println(record.get("status"));
                assertTrue((boolean) record.get("status"));
                resultFound = true;
            }

            tx.success();
        }
        assertTrue(resultFound);
    }

    protected String writeSomeStuffToNeo4j() {
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
}
