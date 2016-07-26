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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.graphaware.integration.es.test.ElasticSearchClient;
import com.graphaware.module.es.mapping.AdvancedMapping;
import io.searchbox.client.JestResult;
import io.searchbox.core.Get;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.neo4j.graphdb.*;

import java.util.*;

import static org.junit.Assert.*;

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
    
    public void verifyEsAdvancedReplication() {
        verifyEsAdvancedReplication(DEFAULT_INDEX_PREFIX);
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
    
    public void verifyEsAdvancedReplication(String indexPrefix) {
        try (Transaction tx = database.beginTx()) {
            for (Node node : database.getAllNodes()) {
                if (configuration.getInclusionPolicies().getNodeInclusionPolicy().include(node)) {
                    verifyEsAdvancedReplication(node, indexPrefix);
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
        String nodeKeyProperty;
        try (Transaction tx = database.beginTx()) {
            nodeKeyProperty = configuration.getMapping().getKeyProperty();
            nodeKey = node.getProperty(nodeKeyProperty).toString();

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
            assertEquals(properties.size() - 1, source.entrySet().size());
            for (String key : properties.keySet()) {
                if (key.equals(nodeKeyProperty)) {
                    assertNull(source.get(key));
                } else if (properties.get(key) instanceof String[]) {
                    checkStringArray(source, key, properties);                
                } else if (properties.get(key) instanceof int[]) {
                    checkIntArray(source, key, properties);                
                } else if (properties.get(key) instanceof char[]) {
                    checkCharArray(source, key, properties);                
                } else if (properties.get(key) instanceof byte[]) {
                    checkByteArray(source, key, properties);                
                } else if (properties.get(key) instanceof boolean[]) {
                    checkBooleanArray(source, key, properties);                
                } else if (properties.get(key) instanceof float[]) {
                    checkFloatArray(source, key, properties);                
                } else if (properties.get(key) instanceof double[]) {
                    checkDoubleArray(source, key, properties);                
                } else if (properties.get(key) instanceof short[]) {
                    checkShortArray(source, key, properties);                
                } else if (properties.get(key) instanceof long[]) {
                    checkLongArray(source, key, properties);                
                }
                else 
                    assertEquals(properties.get(key).toString(), source.get(key).getAsString());
            }
        }
    }

    public JestResult verifyEsReplication(Node node, String index, String type, String keyProperty) {
        String nodeKey;
        try (Transaction tx = database.beginTx()) {
            nodeKey = node.getProperty(keyProperty).toString();
            tx.success();
        }

        Get get = new Get.Builder(index, nodeKey).type(type).build();
        JestResult result = esClient.execute(get);
        System.out.println(result.getJsonString());

        assertTrue(result.getErrorMessage(), result.isSucceeded());
        assertEquals(nodeKey, result.getValue("_id"));

        return result;
    }

    public void verifyNoEsReplication(Node node, String index, String type, String keyProperty) {
        String nodeKey;
        try (Transaction tx = database.beginTx()) {
            nodeKey = node.getProperty(keyProperty).toString();
            tx.success();
        }
        Get get = new Get.Builder(index, nodeKey).type(type).build();
        JestResult result = esClient.execute(get);
        System.out.println(result.getJsonString());

        assertTrue(!result.isSucceeded() || !((Boolean) result.getValue("found")));
    }

    public JestResult verifyEsReplication(Relationship relationship, String index, String type, String keyProperty) {
        String relKey;
        try (Transaction tx = database.beginTx()) {
            relKey = relationship.getProperty(keyProperty).toString();
            tx.success();
        }

        Get get = new Get.Builder(index, relKey).type(type).build();
        JestResult result = esClient.execute(get);
        System.out.println(result.getJsonString());

        assertTrue(result.getErrorMessage(), result.isSucceeded());
        assertEquals(relKey, result.getValue("_id"));

        return result;
    }
    
    public void verifyEsAdvancedReplication(Node node, String indexPrefix) {
        String index = INDEX(true, indexPrefix);
        Map<String, Object> properties = new HashMap<>();
        Set<String> labels = new TreeSet<>();
        String nodeKey;
        String nodeKeyProperty;

        try (Transaction tx = database.beginTx()) {
            nodeKeyProperty = configuration.getKeyProperty();
            nodeKey = node.getProperty(nodeKeyProperty).toString();

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

        Get get = new Get.Builder(index, nodeKey).type(AdvancedMapping.NODE_TYPE).build();
        JestResult result = esClient.execute(get);

        assertTrue(result.getErrorMessage(), result.isSucceeded());
        assertEquals(index, result.getValue("_index"));
        assertEquals(AdvancedMapping.NODE_TYPE, result.getValue("_type"));
        assertEquals(nodeKey, result.getValue("_id"));
        assertTrue((Boolean) result.getValue("found"));

        JsonObject source = result.getJsonObject().getAsJsonObject("_source");
        // field count: "_labels"/"_type" added but "uuid" removed
        assertEquals(properties.size(), source.entrySet().size());
        for (String key : properties.keySet()) {
            if (key.equals(nodeKeyProperty)) {
                assertNull(source.get(key));
            } else if (properties.get(key) instanceof String[]) {
                checkStringArray(source, key, properties);
            } else if (properties.get(key) instanceof int[]) {
                checkIntArray(source, key, properties);
            } else if (properties.get(key) instanceof char[]) {
                checkCharArray(source, key, properties);
            } else if (properties.get(key) instanceof byte[]) {
                checkByteArray(source, key, properties);
            } else if (properties.get(key) instanceof boolean[]) {
                checkBooleanArray(source, key, properties);
            } else if (properties.get(key) instanceof float[]) {
                checkFloatArray(source, key, properties);
            } else if (properties.get(key) instanceof double[]) {
                checkDoubleArray(source, key, properties);
            } else if (properties.get(key) instanceof short[]) {
                checkShortArray(source, key, properties);
            } else if (properties.get(key) instanceof long[]) {
                checkLongArray(source, key, properties);
            } else {
                assertEquals(properties.get(key).toString(), source.get(key).getAsString());
            }
        }
        checkLabels(source, labels);
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
    
    private void checkStringArray(JsonObject source, String key, Map<String, Object> properties) {
        assertTrue(source.get(key) instanceof JsonArray);
        JsonArray jsonArray = source.get(key).getAsJsonArray();
        TreeSet<String> esSet = new TreeSet();
        for (JsonElement element : jsonArray) {
          esSet.add(element.getAsString());
        }
        String[] propertyArray = (String[])properties.get(key);
        TreeSet<String> nodeSet = new TreeSet<>(Arrays.asList(propertyArray));
        assertEquals(esSet, nodeSet);
    }
    
    private void checkIntArray(JsonObject source, String key, Map<String, Object> properties) {
        assertTrue(source.get(key) instanceof JsonArray);
        JsonArray jsonArray = source.get(key).getAsJsonArray();
        TreeSet<Integer> esSet = new TreeSet();
        for (JsonElement element : jsonArray) {
          esSet.add(element.getAsInt());
        }
        TreeSet<Integer> nodeSet = new TreeSet();
        int[] propertyArray = (int[])properties.get(key);
        for (int element : propertyArray) {
          nodeSet.add(element);
        }
        assertEquals(esSet, nodeSet);
    }

    private void checkCharArray(JsonObject source, String key, Map<String, Object> properties) {
        assertTrue(source.get(key) instanceof JsonArray);
        JsonArray jsonArray = source.get(key).getAsJsonArray();
        TreeSet<String> esSet = new TreeSet();
        for (JsonElement element : jsonArray) {
          esSet.add(element.getAsString());
        }
        TreeSet<String> nodeSet = new TreeSet();
        char[] propertyArray = (char[])properties.get(key);
        for (char element : propertyArray) {
          nodeSet.add(String.valueOf(element));
        }
        assertEquals(esSet, nodeSet);
    }

    private void checkByteArray(JsonObject source, String key, Map<String, Object> properties) {
        assertTrue(source.get(key) instanceof JsonArray);
        JsonArray jsonArray = source.get(key).getAsJsonArray();
        TreeSet<Byte> esSet = new TreeSet();
        for (JsonElement element : jsonArray) {
          esSet.add(element.getAsByte());
        }
        TreeSet<Byte> nodeSet = new TreeSet();
        byte[] propertyArray = (byte[])properties.get(key);
        for (byte element : propertyArray) {
          nodeSet.add(element);
        }
        assertEquals(esSet, nodeSet);
    }

    private void checkBooleanArray(JsonObject source, String key, Map<String, Object> properties) {
        assertTrue(source.get(key) instanceof JsonArray);
        JsonArray jsonArray = source.get(key).getAsJsonArray();
        TreeSet<Boolean> esSet = new TreeSet();
        for (JsonElement element : jsonArray) {
          esSet.add(element.getAsBoolean());
        }
        TreeSet<Boolean> nodeSet = new TreeSet();
        boolean[] propertyArray = (boolean[])properties.get(key);
        for (boolean element : propertyArray) {
          nodeSet.add(element);
        }
        assertEquals(esSet, nodeSet);
    }

    private void checkFloatArray(JsonObject source, String key, Map<String, Object> properties) {
        assertTrue(source.get(key) instanceof JsonArray);
        JsonArray jsonArray = source.get(key).getAsJsonArray();
        TreeSet<Float> esSet = new TreeSet();
        for (JsonElement element : jsonArray) {
          esSet.add(element.getAsFloat());
        }
        TreeSet<Float> nodeSet = new TreeSet();
        float[] propertyArray = (float[])properties.get(key);
        for (float element : propertyArray) {
          nodeSet.add(element);
        }
        assertEquals(esSet, nodeSet);
    }

    private void checkDoubleArray(JsonObject source, String key, Map<String, Object> properties) {
        assertTrue(source.get(key) instanceof JsonArray);
        JsonArray jsonArray = source.get(key).getAsJsonArray();
        TreeSet<Double> esSet = new TreeSet();
        for (JsonElement element : jsonArray) {
          esSet.add(element.getAsDouble());
        }
        TreeSet<Double> nodeSet = new TreeSet();
        double[] propertyArray = (double[])properties.get(key);
        for (double element : propertyArray) {
          nodeSet.add(element);
        }
        assertEquals(esSet, nodeSet);
    }

    private void checkShortArray(JsonObject source, String key, Map<String, Object> properties) {
        assertTrue(source.get(key) instanceof JsonArray);
        JsonArray jsonArray = source.get(key).getAsJsonArray();
        TreeSet<Short> esSet = new TreeSet();
        for (JsonElement element : jsonArray) {
          esSet.add(element.getAsShort());
        }
        TreeSet<Short> nodeSet = new TreeSet();
        short[] propertyArray = (short[])properties.get(key);
        for (short element : propertyArray) {
          nodeSet.add(element);
        }
        assertEquals(esSet, nodeSet);
    }

    private void checkLongArray(JsonObject source, String key, Map<String, Object> properties) {
        assertTrue(source.get(key) instanceof JsonArray);
        JsonArray jsonArray = source.get(key).getAsJsonArray();
        TreeSet<Long> esSet = new TreeSet();
        for (JsonElement element : jsonArray) {
          esSet.add(element.getAsLong());
        }
        TreeSet<Long> nodeSet = new TreeSet();
        long[] propertyArray = (long[])properties.get(key);
        for (long element : propertyArray) {
          nodeSet.add(element);
        }
        assertEquals(esSet, nodeSet);
    }

    private void checkLabels(JsonObject source, Set labels) {
        JsonArray jsonArray = source.get(AdvancedMapping.LABELS_FIELD).getAsJsonArray();
        TreeSet<String> esSet = new TreeSet();
        for (JsonElement element : jsonArray) {
          esSet.add(element.getAsString());
        }
        assertEquals(esSet, labels);
    }
}
