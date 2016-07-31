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
import org.neo4j.graphdb.Transaction;
import org.skyscreamer.jsonassert.JSONAssert;

import static com.graphaware.module.es.util.TestUtil.waitFor;
import static org.junit.Assert.assertTrue;

public class ElasticSearchModuleDefaultMappingTest extends GraphAwareIntegrationTest {

    protected ElasticSearchServer esServer;

    @Override
    protected String configFile() {
        return "integration/issue-29.conf";
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
    public void testCustomIndexNameIsUsedWhenNoSpecificMappingClassDefined() throws JSONException {
        String uuid = writeSomeStuffToNeo4j();
        System.out.println(uuid);
        waitFor(1500);
        ElasticSearchClient esClient = new JestElasticSearchClient("localhost", "9201");
        Get get = new Get.Builder("different-index-name-node", uuid).type("Person").build();
        JestResult result = esClient.execute(get);
        System.out.println(result.getJsonString());
        assertTrue(result.isSucceeded());
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
