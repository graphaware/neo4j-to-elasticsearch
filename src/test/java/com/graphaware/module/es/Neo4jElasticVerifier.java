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

import com.google.gson.JsonObject;
import com.graphaware.integration.es.test.ElasticSearchClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Get;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Neo4jElasticVerifier {

    public static String DEFAULT_INDEX_PREFIX = "neo4j-index";

    public static String INDEX(boolean node, String prefix) {
        return prefix + (node ? "-node" : "-relationship");
    }

    private final GraphDatabaseService database;
    private final ElasticSearchConfiguration configuration;
    private final ElasticSearchClient esClient;

    public Neo4jElasticVerifier(GraphDatabaseService database, ElasticSearchConfiguration configuration, ElasticSearchClient esClient) {
        this.database = database;
        this.configuration = configuration;
        this.esClient = esClient;
    }

    public void verifyEsReplication() {
        verifyEsReplication(DEFAULT_INDEX_PREFIX);
    }

    /**
     * @param indexPrefix non-suffixed index name
     */
    public void verifyEsReplication(String indexPrefix) {
        try (Transaction tx = database.beginTx()) {
            for (Node node : database.getAllNodes()) {
                if (configuration.getInclusionPolicies().getNodeInclusionPolicy().include(node)) {
                    verifyEsReplication(node, indexPrefix);
                }
            }
            tx.success();
        }
    }

    public void verifyNoEsReplication() {
        try (Transaction tx = database.beginTx()) {
            for (Node node : database.getAllNodes()) {
                verifyNoEsReplication(node);
            }
            tx.success();
        }
    }

    public void verifyEsReplication(Node node) {
        verifyEsReplication(node, DEFAULT_INDEX_PREFIX);
    }

    /**
     * @param node a node
     * @param indexPrefix non-suffixed index name
     */
    public void verifyEsReplication(Node node, String indexPrefix) {
        String index = INDEX(true, indexPrefix);
        Map<String, Object> properties = new HashMap<>();
        Set<String> labels = new HashSet<>();
        String nodeKey;
        try (Transaction tx = database.beginTx()) {
            nodeKey = node.getProperty(configuration.getKeyProperty()).toString();

            for (String key : node.getPropertyKeys()) {
                if (configuration.getInclusionPolicies().getNodePropertyInclusionPolicy().include(key, node)) {
                    properties.put(key, node.getProperty(key));
                }
            }

            for (Label label : node.getLabels()) {
                labels.add(label.name());
            }

            tx.success();
        }

        for (String label : labels) {
            Get get = new Get.Builder(index, nodeKey).type(label).build();
            JestResult result = esClient.execute(get);

            assertTrue(result.getErrorMessage(), result.isSucceeded());
            assertEquals(index, result.getValue("_index"));
            assertEquals(label, result.getValue("_type"));
            assertEquals(nodeKey, result.getValue("_id"));
            assertTrue((Boolean) result.getValue("found"));

            JsonObject source = result.getJsonObject().getAsJsonObject("_source");
            assertEquals(properties.size(), source.entrySet().size());
            for (String key : properties.keySet()) {
                assertEquals(properties.get(key).toString(), source.get(key).getAsString());
            }
        }
    }

    public void verifyNoEsReplication(Node node) {
        String index = INDEX(true, DEFAULT_INDEX_PREFIX);
        Set<String> labels = new HashSet<>();
        String nodeKey;
        try (Transaction tx = database.beginTx()) {
            if (configuration != null) {
                nodeKey = node.getProperty(configuration.getKeyProperty()).toString();
            } else {
                nodeKey = node.getProperty("uuid", "unknown").toString();
            }

            for (Label label : node.getLabels()) {
                labels.add(label.name());
            }

            tx.success();
        }

        for (String label : labels) {
            Get get = new Get.Builder(index, nodeKey).type(label).build();
            JestResult result = esClient.execute(get);

            assertTrue(!result.isSucceeded() || !((Boolean) result.getValue("found")));
        }
    }

    public void verifyEsEmpty() {
        assertEquals(0, countNodes());
    }

    public int countNodes() {
        Set<Label> labels = new HashSet<>();

        try (Transaction tx = database.beginTx()) {
            for (Label label : database.getAllLabels()) {
                labels.add(label);
                tx.success();
            }
        }

        return countNodes(labels.toArray(new Label[labels.size()]));
    }

    public int countNodes(Label... labels) {
        String index = INDEX(true, DEFAULT_INDEX_PREFIX);
        String query = "{"
                + "   \"filter\": {"
                + "      \"bool\": {"
                + "         \"must\": ["
                + "            {"
                + "                  \"match_all\": {}"
                + "            }"
                + "         ]"
                + "      }"
                + "   }"
                + "}";

        int count = 0;

        for (Label label : labels) {
            Search search = new Search.Builder(query).addIndex(index)
                    .addType(label.name())
                    .setParameter("size", 10_000)
                    .build();
            SearchResult result = esClient.execute(search);

            if (result.isSucceeded()) {
                List<SearchResult.Hit<JestUUIDResult, Void>> hits = result.getHits(JestUUIDResult.class);
                count += hits.size();
            }
        }

        return count;
    }
}
