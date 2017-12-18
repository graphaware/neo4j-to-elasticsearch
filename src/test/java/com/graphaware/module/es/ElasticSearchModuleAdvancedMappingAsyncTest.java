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
import com.graphaware.module.es.mapping.AdvancedMapping;
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

public class ElasticSearchModuleAdvancedMappingAsyncTest extends ElasticSearchModuleIntegrationTest {

    @Test
    public void testArray() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration(), database));

        Mapping mapping = ServiceLoader.loadMapping(AdvancedMapping.class.getCanonicalName());

        configuration = ElasticSearchConfiguration.defaultConfiguration()
                .withMapping(mapping, new HashMap<>())
                .withUri(HOST)
                .withPort(PORT)
                .withAsyncIndexation(true);
        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();

        writeSomeStuffWithListToNeo4j();
        // leaving a little bit more time for indexation to finish since it's done in a thread
        TestUtil.waitFor(1000);
        verifyEsAdvancedReplication();
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
