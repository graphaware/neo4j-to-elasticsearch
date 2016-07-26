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
import com.graphaware.test.integration.GraphAwareIntegrationTest;
import io.searchbox.client.JestResult;
import io.searchbox.core.Get;
import org.json.JSONException;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import static com.graphaware.module.es.util.TestUtil.waitFor;

public class ElasticSearchModuleEndToEndSimpleMappingTest extends GraphAwareIntegrationTest {

    protected ElasticSearchServer esServer;

    @Override
    protected String configFile() {
        return "integration/int-test-simple.conf";
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
        waitFor(500);
        ElasticSearchClient esClient = new JestElasticSearchClient("localhost", "9201");
        Get get = new Get.Builder("neo4j-index-node", uuid).type("Person").build();
        JestResult result = esClient.execute(get);
        System.out.println(result.getJsonString());
        JSONAssert.assertEquals(
                "{\"_index\":\"neo4j-index-node\",\"_type\":\"Person\",\"_id\":\"" + uuid + "\",\"_version\":2,\"found\":true,\"_source\":{\"age\":\"31\",\"name\":\"Michal\"}}",
                result.getJsonString(),
                false
        );
    }

    protected String writeSomeStuffToNeo4j() {
        //tx1
        httpClient.executeCypher(baseNeoUrl(), "CREATE (p:Person {name:'Michal', age:30})-[:WORKS_FOR {since:2013, role:'MD'}]->(c:Company {name:'GraphAware', est: 2013})");

        //tx2
        httpClient.executeCypher(baseNeoUrl(), "MATCH (ga:Company {name:'GraphAware'}) CREATE (p:Person {name:'Adam'})-[:WORKS_FOR {since:2014}]->(ga)");

        //tx3
        httpClient.executeCypher(baseNeoUrl(),
                "MATCH (ga:Company {name:'GraphAware'}) CREATE (p:Person {name:'Daniela'})-[:WORKS_FOR]->(ga)",
                "MATCH (p:Person {name:'Michal'}) SET p.age=31",
                "MATCH (p:Person {name:'Adam'})-[r]-() DELETE p,r",
                "MATCH (p:Person {name:'Michal'})-[r:WORKS_FOR]->() REMOVE r.role");

        return httpClient.executeCypher(baseNeoUrl(), "MATCH (p:Person {name:'Michal'}) RETURN p.uuid").replace("{\"results\":[{\"columns\":[\"p.uuid\"],\"data\":[{\"row\":[\"", "").replace("\"],\"meta\":[null]}]}],\"errors\":[]}", "");
    }
}
