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
import static com.graphaware.module.es.ElasticSearchModuleIntegrationTest.HOST;
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


public class ElasticSearchModuleArrayTest extends ElasticSearchModuleIntegrationTest {

    @Test
    public void testArray() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration(), database));

        configuration = ElasticSearchConfiguration.defaultConfiguration().withUri(HOST).withPort(PORT);
        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();

        writeSomeStuffWithArrayToNeo4j();
        TestUtil.waitFor(1000);
        verifyEsReplication();

        writeSomeStuffWithListToNeo4j();
        TestUtil.waitFor(1000);
        verifyEsReplication();
    }

    protected void writeSomeStuffWithArrayToNeo4j() {
        //tx1
        database.execute("CREATE (p:Person {name:'Michal', age:30})-[:WORKS_FOR {since:2013, role:'MD'}]->(c:Company {name:'GraphAware', est: 2013, roles:[\"CEO\", \"CTO\"]})");
    }
    
    protected void writeSomeStuffWithListToNeo4j() {
        //tx2
        try (Transaction tx = database.beginTx()) {
            Node node = database.createNode(Label.label("LabelWithList"));
            int[] listOfInteger = {1, 2, 3};
            node.setProperty("listOfInteger", listOfInteger);

            String[] listOfString = {"1", "2", "3"};
            node.setProperty("listOfString", listOfString);            
            
            long[] listOfLong = {1l, 2l, 3l,};
            node.setProperty("listOfLong", listOfLong);
            
            float[] listOfFloat = {1.0f, 2.1f, 3.0f};
            node.setProperty("listOfFloat", listOfFloat);
            
            double[] listOfDouble = {1.0d, 2.1d, 3.0d};
            node.setProperty("listOfDouble", listOfDouble);
            
            //Not supported
//            char[] listOfChar = {'a', 'b', 'c'};
//            node.setProperty("listOfChar", listOfChar);
            //Not supported
//            byte[] listOfByte = {'a', 'b', 'c'};
//            node.setProperty("listOfByte", listOfByte);
            
            short[] listOfShort = {1, 2, 3};
            node.setProperty("listOfShort", listOfShort);
            
            boolean[] listOfBoolean = {true, false, true};
            node.setProperty("listOfBoolean", listOfBoolean);
            tx.success();
        }
        
    }
}
