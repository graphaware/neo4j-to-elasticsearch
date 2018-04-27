package com.graphaware.module.es;

import com.graphaware.common.policy.inclusion.all.IncludeAllRelationships;
import com.graphaware.integration.es.test.EmbeddedElasticSearchServer;
import com.graphaware.integration.es.test.JestElasticSearchClient;
import com.graphaware.module.es.mapping.JsonFileMapping;
import com.graphaware.module.es.mapping.Mapping;
import com.graphaware.module.es.util.ServiceLoader;
import com.graphaware.module.es.util.TestUtil;
import static com.graphaware.module.es.util.TestUtil.waitFor;
import com.graphaware.module.uuid.UuidConfiguration;
import com.graphaware.module.uuid.UuidModule;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import io.searchbox.client.JestResult;
import io.searchbox.core.DeleteByQuery;
import io.searchbox.core.Get;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class ElasticSearchModuleJsonMappingTest extends ElasticSearchModuleIntegrationTest {

    @Before
    public void setUp() {
        esServer = new EmbeddedElasticSearchServer();
        esServer.start();
        esClient = new JestElasticSearchClient(HOST, PORT);
    }

    @Test
    public void overallTest() throws IOException {
        testBasicJsonMappingModuleBootstrap();
        cleanUpData();
        testBasicJsonMappingReplication();
        cleanUpData();
        testJsonMappingWithMultipleMappingsAndMoreThanOneLabelAndIndex();
        cleanUpData();
        testShouldReplicateNodesWithoutLabels();
        cleanUpData();
        testNodesWithArrayPropertyValuesShouldBeReplicatedCorrectly();
        cleanUpData();
        testNodesWithMultipleLabelsAreUpdatedCorrectly();
        cleanUpData();
        testDeleteNodesAreDeletedFromIndices();
        cleanUpData();
        testDeletedRelationshipsAreDeletedFromIndices();
        cleanUpData();
        testTimeBasedIndex();
        cleanUpData();
        testBlacklistedPropertiesAreNotIndexed();
        cleanUpData();
        testRelationshipsAreUpdatedAndRemovedIfNeeded();
        cleanUpData();
        testDynamicTypesAreCorrectlyHandled();
        cleanUpData();
        testInvalidJsonMappingDoesNotFailTransaction();
        cleanUpData();
        testTypeOfRelationshipCanBeUsedAsFieldContent();
        cleanUpData();
        testPropertyValuesTransformation();
        cleanUpData();
        testIndexWithAllNodesAndAllRelsExpression();
        cleanUpData();
        testBasicJsonMappingReplicationWithQuery();
        cleanUpData();
    }

    //@Test
    public void testBasicJsonMappingModuleBootstrap() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration(), database));

        Mapping mapping = ServiceLoader.loadMapping("com.graphaware.module.es.mapping.JsonFileMapping");
        Map<String, String> mappingConfig = new HashMap<>();
        mappingConfig.put("file", "integration/mapping-basic.json");

        configuration = ElasticSearchConfiguration.defaultConfiguration()
                .withMapping(mapping, mappingConfig)
                .withUri(HOST)
                .withPort(PORT);

        assertEquals("uuid", configuration.getMapping().getKeyProperty());
        assertEquals("default-index-node", ((JsonFileMapping)configuration.getMapping()).getMappingRepresentation().getDefaults().getDefaultNodesIndex());
        assertEquals("default-index-relationship", ((JsonFileMapping)configuration.getMapping()).getMappingRepresentation().getDefaults().getDefaultRelationshipsIndex());
    }

    //@Test
    public void testBasicJsonMappingReplication() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration().withUuidProperty("uuid").with(IncludeAllRelationships.getInstance()), database));

        JsonFileMapping mapping = (JsonFileMapping) ServiceLoader.loadMapping("com.graphaware.module.es.mapping.JsonFileMapping");
        Map<String, String> mappingConfig = new HashMap<>();
        mappingConfig.put("file", "integration/mapping-basic.json");

        configuration = ElasticSearchConfiguration.defaultConfiguration()
                .withMapping(mapping, mappingConfig)
                .with(IncludeAllRelationships.getInstance())
                .withUri(HOST)
                .withPort(PORT);

        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();

        writeSomePersons();
        TestUtil.waitFor(2000);
        verifyEsReplicationForNodeWithLabels("Person", mapping.getMappingRepresentation().getDefaults().getDefaultNodesIndex(), "persons", mapping.getMappingRepresentation().getDefaults().getKeyProperty());
        try (Transaction tx = database.beginTx()) {
            database.getAllRelationships().stream().forEach(r -> {
                new Neo4jElasticVerifier(database, configuration, esClient).verifyEsReplication(r, mapping.getMappingRepresentation().getDefaults().getDefaultRelationshipsIndex(), "workers", mapping.getKeyProperty());
            });
            tx.success();
        }
    }

    //@Test
    public void testJsonMappingWithMultipleMappingsAndMoreThanOneLabelAndIndex() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration().withUuidProperty("uuid"), database));

        JsonFileMapping mapping = (JsonFileMapping) ServiceLoader.loadMapping("com.graphaware.module.es.mapping.JsonFileMapping");
        Map<String, String> mappingConfig = new HashMap<>();
        mappingConfig.put("file", "integration/mapping-multi-labels.json");

        configuration = ElasticSearchConfiguration.defaultConfiguration()
                .withMapping(mapping, mappingConfig)
                .withUri(HOST)
                .withPort(PORT);

        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();

        writeSomePersons();
        TestUtil.waitFor(1000);
        try (Transaction tx = database.beginTx()) {
            database.findNodes(Label.label("Female")).stream().forEach(n -> {
                new Neo4jElasticVerifier(database, configuration, esClient).verifyEsReplication(n, "females", "girls", mapping.getKeyProperty());
            });
            database.findNodes(Label.label("Person")).stream().forEach(n -> {
                new Neo4jElasticVerifier(database, configuration, esClient).verifyEsReplication(n, mapping.getMappingRepresentation().getDefaults().getDefaultNodesIndex(), "persons", mapping.getKeyProperty());
            });
            tx.success();
        }
    }

    //@Test
    public void testShouldReplicateNodesWithoutLabels() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration().withUuidProperty("uuid"), database));

        JsonFileMapping mapping = (JsonFileMapping) ServiceLoader.loadMapping("com.graphaware.module.es.mapping.JsonFileMapping");
        Map<String, String> mappingConfig = new HashMap<>();
        mappingConfig.put("file", "integration/mapping-advanced.json");

        configuration = ElasticSearchConfiguration.defaultConfiguration()
                .withMapping(mapping, mappingConfig)
                .withUri(HOST)
                .withPort(PORT);

        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();

        writeSomePersons();
        TestUtil.waitFor(1000);
        try (Transaction tx = database.beginTx()) {
            database.getAllNodes().stream()
                    .filter(n -> {
                return labelsToStrings(n).size() == 0;
            })
                    .forEach(n -> {
                        new Neo4jElasticVerifier(database, configuration, esClient).verifyEsReplication(n, mapping.getMappingRepresentation().getDefaults().getDefaultNodesIndex(), "nodes-without-labels", mapping.getKeyProperty());
                    });
            tx.success();
        }
    }

    //@Test
    public void testNodesWithArrayPropertyValuesShouldBeReplicatedCorrectly() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration().withUuidProperty("uuid"), database));

        JsonFileMapping mapping = (JsonFileMapping) ServiceLoader.loadMapping("com.graphaware.module.es.mapping.JsonFileMapping");
        Map<String, String> mappingConfig = new HashMap<>();
        mappingConfig.put("file", "integration/mapping-advanced.json");

        configuration = ElasticSearchConfiguration.defaultConfiguration()
                .withMapping(mapping, mappingConfig)
                .withUri(HOST)
                .withPort(PORT);

        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();
        database.execute("CREATE (n:Node {types:['a','b','c']})");
        TestUtil.waitFor(1500);
        try (Transaction tx = database.beginTx()) {
            database.findNodes(Label.label("Node")).stream().forEach(n -> {
                JestResult result = new Neo4jElasticVerifier(database, configuration, esClient).verifyEsReplication(n, mapping.getMappingRepresentation().getDefaults().getDefaultNodesIndex(), "nodes", mapping.getKeyProperty());
                Map<String, Object> source = new HashMap<>();
                List<String> types = (List<String>) result.getSourceAsObject(source.getClass()).get("types");
                assertEquals(3, types.size());
                assertEquals("a", types.get(0));
                assertEquals("c", types.get(2));
            });
            tx.success();
        }
    }

    //@Test
    public void testNodesWithMultipleLabelsAreUpdatedCorrectly() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration().withUuidProperty("uuid"), database));

        JsonFileMapping mapping = (JsonFileMapping) ServiceLoader.loadMapping("com.graphaware.module.es.mapping.JsonFileMapping");
        Map<String, String> mappingConfig = new HashMap<>();
        mappingConfig.put("file", "integration/mapping-advanced.json");

        configuration = ElasticSearchConfiguration.defaultConfiguration()
                .withMapping(mapping, mappingConfig)
                .withUri(HOST)
                .withPort(PORT);

        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();
        writeSomePersons();
        TestUtil.waitFor(1000);
        try (Transaction tx = database.beginTx()) {
            database.findNodes(Label.label("Person")).stream()
                    .filter(n -> { return n.hasLabel(Label.label("Female"));})
                    .forEach(n -> {
                        new Neo4jElasticVerifier(database, configuration, esClient).verifyEsReplication(n, "females", "girls", mapping.getKeyProperty());
                    });
            tx.success();
        }

        database.execute("MATCH (n:Female) REMOVE n:Female SET n:Node");
        TestUtil.waitFor(1000);
        verifyNoEsReplicationForNodesWithLabel("Person", "females", "girls", mapping.getKeyProperty());


    }

    //@Test
    public void testDeleteNodesAreDeletedFromIndices() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration().withUuidProperty("uuid"), database));

        JsonFileMapping mapping = (JsonFileMapping) ServiceLoader.loadMapping("com.graphaware.module.es.mapping.JsonFileMapping");
        Map<String, String> mappingConfig = new HashMap<>();
        mappingConfig.put("file", "integration/mapping-advanced.json");

        configuration = ElasticSearchConfiguration.defaultConfiguration()
                .withMapping(mapping, mappingConfig)
                .withUri(HOST)
                .withPort(PORT);

        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();
        database.execute("CREATE (n:Person {name:\"Person1\"}), (:Person {name:\"Person2\"})");
        TestUtil.waitFor(1000);
        List<String> ids = new ArrayList<>();
        try (Transaction tx = database.beginTx()) {
            Iterator<Node> nodes = database.findNodes(Label.label("Person"));
            while (nodes.hasNext()) {
                Node n = nodes.next();
                ids.add(n.getProperty("uuid").toString());
            }

            tx.success();
        }

        verifyEsReplicationForNodeWithLabels("Person", ((JsonFileMapping) configuration.getMapping()).getMappingRepresentation().getDefaults().getDefaultNodesIndex(), "persons", configuration.getMapping().getKeyProperty());
        database.execute("MATCH (n) DETACH DELETE n");
        TestUtil.waitFor(1000);
        for (String id : ids) {
            String index = ((JsonFileMapping) configuration.getMapping()).getMappingRepresentation().getDefaults().getDefaultNodesIndex();
            String type = "persons";
            Get get = new Get.Builder(index, id).type(type).build();
            JestResult result = esClient.execute(get);

            assertTrue(!result.isSucceeded() || !((Boolean) result.getValue("found")));
        }

    }

    //@Test
    public void testDeletedRelationshipsAreDeletedFromIndices() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration().withUuidProperty("uuid").with(IncludeAllRelationships.getInstance()), database));

        JsonFileMapping mapping = (JsonFileMapping) ServiceLoader.loadMapping("com.graphaware.module.es.mapping.JsonFileMapping");
        Map<String, String> mappingConfig = new HashMap<>();
        mappingConfig.put("file", "integration/mapping-advanced.json");

        configuration = ElasticSearchConfiguration.defaultConfiguration()
                .withMapping(mapping, mappingConfig)
                .with(IncludeAllRelationships.getInstance())
                .withUri(HOST)
                .withPort(PORT);

        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();
        database.execute("CREATE (n:Person {name:\"Person1\"}), (n2:Person {name:\"Person2\"}), (n)-[:KNOWS]->(n2)");
        TestUtil.waitFor(1500);
        List<String> ids = new ArrayList<>();
        try (Transaction tx = database.beginTx()) {
            ResourceIterator<Relationship> rels = database.getAllRelationships().iterator();
            while (rels.hasNext()) {
                Relationship r = rels.next();
                ids.add(r.getProperty("uuid").toString());
                String index = ((JsonFileMapping) configuration.getMapping()).getMappingRepresentation().getDefaults().getDefaultRelationshipsIndex();
                Get get = new Get.Builder(index, r.getProperty("uuid").toString()).type("knowers").build();
                JestResult result = esClient.execute(get);
                assertTrue(result.isSucceeded() || ((Boolean) result.getValue("found")));
            }

            tx.success();
        }


        database.execute("MATCH ()-[r]-() DELETE r");
        TestUtil.waitFor(1500);

        for (String id : ids) {
            String index = ((JsonFileMapping) configuration.getMapping()).getMappingRepresentation().getDefaults().getDefaultRelationshipsIndex();
            Get get = new Get.Builder(index, id).type("knowers").build();
            JestResult result = esClient.execute(get);
            assertTrue(!result.isSucceeded() || !((Boolean) result.getValue("found")));
        }


    }

    //@Test
    public void testTimeBasedIndex() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration().withUuidProperty("uuid").with(IncludeAllRelationships.getInstance()), database));

        JsonFileMapping mapping = (JsonFileMapping) ServiceLoader.loadMapping("com.graphaware.module.es.mapping.JsonFileMapping");
        Map<String, String> mappingConfig = new HashMap<>();
        mappingConfig.put("file", "integration/mapping-advanced.json");

        configuration = ElasticSearchConfiguration.defaultConfiguration()
                .withMapping(mapping, mappingConfig)
                .with(IncludeAllRelationships.getInstance())
                .withUri(HOST)
                .withPort(PORT);

        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();
        Long ts = 1469369250000L; // 07/24/2016 @ 2:07pm (UTC)
        database.execute("CREATE (n:Tweet {timestamp: " + ts + "})");
        TestUtil.waitFor(1500);

        try (Transaction tx = database.beginTx()) {
            database.findNodes(Label.label("Tweet")).stream().forEach(n -> {
                Get get = new Get.Builder("tweets_2016_07_24", n.getProperty("uuid").toString()).type("tweets").build();
                JestResult result = esClient.execute(get);
                System.out.println(result.getJsonString());
                assertTrue(result.isSucceeded() && ((Boolean) result.getValue("found")));
            });
            tx.success();
        }
    }

    //@Test
    public void testBlacklistedPropertiesAreNotIndexed() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration().withUuidProperty("uuid").with(IncludeAllRelationships.getInstance()), database));

        JsonFileMapping mapping = (JsonFileMapping) ServiceLoader.loadMapping("com.graphaware.module.es.mapping.JsonFileMapping");
        Map<String, String> mappingConfig = new HashMap<>();
        mappingConfig.put("file", "integration/mapping-advanced.json");

        configuration = ElasticSearchConfiguration.defaultConfiguration()
                .withMapping(mapping, mappingConfig)
                .with(IncludeAllRelationships.getInstance())
                .withUri(HOST)
                .withPort(PORT);

        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();
        database.execute("CREATE (n:User {login: 'ikwattro', password:'s3cr3t'})-[:WORKS_AT {since:'2014-11-15'}]->(c:Company {name:'GraphAware'})");
        TestUtil.waitFor(1500);

        try (Transaction tx = database.beginTx()) {
            database.findNodes(Label.label("User")).stream().forEach(n -> {
                Get get = new Get.Builder("node-index", n.getProperty("uuid").toString()).type("users").build();
                JestResult result = esClient.execute(get);
                System.out.println(result.getJsonString());

                assertTrue(!result.getSourceAsObject(Map.class).containsKey("password"));
                assertTrue(result.getSourceAsObject(Map.class).containsKey("login"));
            });

            database.getAllRelationships().stream().forEach(r -> {
                Get get = new Get.Builder("relationship-index", r.getProperty("uuid").toString()).type("workers").build();
                JestResult result = esClient.execute(get);
                System.out.println(result.getJsonString());
                assertTrue(result.isSucceeded());
                assertTrue(!result.getSourceAsObject(Map.class).containsKey("uuid"));
                assertTrue(result.getSourceAsObject(Map.class).containsKey("since"));
            });

            tx.success();
        }
    }

    //@Test
    public void testIndexWithAllNodesAndAllRelsExpression() throws IOException {
        esServer.stop();
        esServer = new EmbeddedElasticSearchServer();
        esServer.start();

        TemporaryFolder folder = new TemporaryFolder();
        folder.create();
        folder.getRoot().deleteOnExit();
        
        database = new GraphDatabaseFactory().newEmbeddedDatabase(folder.getRoot());

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration().withUuidProperty("uuid").with(IncludeAllRelationships.getInstance()), database));

        JsonFileMapping mapping = (JsonFileMapping) ServiceLoader.loadMapping("com.graphaware.module.es.mapping.JsonFileMapping");
        Map<String, String> mappingConfig = new HashMap<>();
        mappingConfig.put("file", "integration/mapping-all.json");

        configuration = ElasticSearchConfiguration.defaultConfiguration()
                .withMapping(mapping, mappingConfig)
                .with(IncludeAllRelationships.getInstance())
                .withUri(HOST)
                .withPort(PORT);

        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();
        database.execute("CREATE (n:User {login: 'ikwattro', password:'s3cr3t'})-[:WORKS_AT {since:'2014-11-15'}]->(c:Company {name:'GraphAware'})");
        TestUtil.waitFor(2000);

        try (Transaction tx = database.beginTx()) {
            database.findNodes(Label.label("User")).stream().forEach(n -> {
                Get get = new Get.Builder("default-index-node", n.getProperty("uuid").toString()).type("nodes").build();
                JestResult result = esClient.execute(get);
                assertTrue(result.isSucceeded());

            });

            database.getAllRelationships().stream().forEach(r -> {
                Get get = new Get.Builder("default-index-relationship", r.getProperty("uuid").toString()).type("relationships").build();
                JestResult result = esClient.execute(get);
                assertEquals(true, result.isSucceeded());
            });

            tx.success();
        }
    }

    //@Test
    public void testRelationshipsAreUpdatedAndRemovedIfNeeded() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration().withUuidProperty("uuid").with(IncludeAllRelationships.getInstance()), database));

        JsonFileMapping mapping = (JsonFileMapping) ServiceLoader.loadMapping("com.graphaware.module.es.mapping.JsonFileMapping");
        Map<String, String> mappingConfig = new HashMap<>();
        mappingConfig.put("file", "integration/mapping-advanced.json");

        configuration = ElasticSearchConfiguration.defaultConfiguration()
                .withMapping(mapping, mappingConfig)
                .with(IncludeAllRelationships.getInstance())
                .withUri(HOST)
                .withPort(PORT);

        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();
        database.execute("CREATE (n:User {login: 'ikwattro', password:'s3cr3t'})-[:CLICKS_ON {count: 10}]->(c:Company {name:'GraphAware'})");
        TestUtil.waitFor(1500);
        String relIndex = ((JsonFileMapping) configuration.getMapping()).getMappingRepresentation().getDefaults().getDefaultRelationshipsIndex();
        try (Transaction tx = database.beginTx()) {
            database.getAllRelationships().stream().forEach(r -> {
                Get get = new Get.Builder(relIndex, r.getProperty("uuid").toString()).type("clicks").build();
                JestResult result = esClient.execute(get);
                assertTrue(result.isSucceeded());
            });

            tx.success();
        }
        database.execute("MATCH ()-[r]-() REMOVE r.count");
        TestUtil.waitFor(1000);
        try (Transaction tx = database.beginTx()) {
            database.getAllRelationships().stream().forEach(r -> {
                Get get = new Get.Builder(relIndex, r.getProperty("uuid").toString()).type("clicks").build();
                JestResult result = esClient.execute(get);
                assertTrue(!result.isSucceeded());
            });
            tx.success();
        }
    }

    //@Test
    public void testDynamicTypesAreCorrectlyHandled() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration().withUuidProperty("uuid").with(IncludeAllRelationships.getInstance()), database));

        JsonFileMapping mapping = (JsonFileMapping) ServiceLoader.loadMapping("com.graphaware.module.es.mapping.JsonFileMapping");
        Map<String, String> mappingConfig = new HashMap<>();
        mappingConfig.put("file", "integration/mapping-advanced.json");

        configuration = ElasticSearchConfiguration.defaultConfiguration()
                .withMapping(mapping, mappingConfig)
                .with(IncludeAllRelationships.getInstance())
                .withUri(HOST)
                .withPort(PORT);

        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();
        database.execute("CREATE (n:DynaType {action: 'pull'})");
        TestUtil.waitFor(1500);
        try (Transaction tx = database.beginTx()) {
            database.findNodes(Label.label("DynaType")).stream().forEach(n -> {
                String index = ((JsonFileMapping) configuration.getMapping()).getMappingRepresentation().getDefaults().getDefaultNodesIndex();
                String type = n.getProperty("action").toString();
                Get get = new Get.Builder(index, n.getProperty("uuid").toString()).type(type).build();
                JestResult result = esClient.execute(get);
                System.out.println(result.getJsonString());
                assertTrue(result.isSucceeded());
            });

            tx.success();
        }
    }

    //@Test
    public void testInvalidJsonMappingDoesNotFailTransaction() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration().withUuidProperty("uuid").with(IncludeAllRelationships.getInstance()), database));

        JsonFileMapping mapping = (JsonFileMapping) ServiceLoader.loadMapping("com.graphaware.module.es.mapping.JsonFileMapping");
        Map<String, String> mappingConfig = new HashMap<>();
        mappingConfig.put("file", "integration/mapping-invalid.json");

        configuration = ElasticSearchConfiguration.defaultConfiguration()
                .withMapping(mapping, mappingConfig)
                .with(IncludeAllRelationships.getInstance())
                .withUri(HOST)
                .withPort(PORT);

        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();
        database.execute("CREATE (n:Person {name:'John Doe'})-[:WORKS_FOR]->(c:Company {name:'Acme'})<-[:WORKED_AT {since: 123456}]-(x:Person {name:'Rocko Balboi'})");
        TestUtil.waitFor(1500);
        try (Transaction tx = database.beginTx()) {
            int i = 0;
            Iterator<Node> it = database.findNodes(Label.label("Company"));
            while (it.hasNext()) {
                Node node = it.next();
                ++i;
                Get get = new Get.Builder("default-index-node", node.getProperty("uuid").toString()).type("companies").build();
                JestResult result = esClient.execute(get);
                assertTrue(result.isSucceeded());
            }
            assertEquals(1, i);

            int z = 0;
            Iterator<Relationship> itr = database.getAllRelationships().iterator();
            while (itr.hasNext()) {
                Relationship r = itr.next();
                if (r.getType().toString().equals("WORKED_AT")) {
                    ++z;
                    Get get = new Get.Builder("default-index-relationship", r.getProperty("uuid").toString()).type("old-workers").build();
                    JestResult result = esClient.execute(get);
                    assertTrue(result.isSucceeded());
                }
            }
            assertEquals(1, z);

            tx.success();
        }

    }

    //@Test
    public void testTypeOfRelationshipCanBeUsedAsFieldContent() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration().withUuidProperty("uuid").with(IncludeAllRelationships.getInstance()), database));

        JsonFileMapping mapping = (JsonFileMapping) ServiceLoader.loadMapping("com.graphaware.module.es.mapping.JsonFileMapping");
        Map<String, String> mappingConfig = new HashMap<>();
        mappingConfig.put("file", "integration/mapping-advanced.json");

        configuration = ElasticSearchConfiguration.defaultConfiguration()
                .withMapping(mapping, mappingConfig)
                .with(IncludeAllRelationships.getInstance())
                .withUri(HOST)
                .withPort(PORT);

        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();
        database.execute("CREATE (n:Person {name:'John Doe'})-[:RELATED_TO]->(c:Company {name:'Acme'})");
        TestUtil.waitFor(1500);

        try (Transaction tx = database.beginTx()) {
            for (Relationship rel : database.getAllRelationships()) {
                String uuid = rel.getProperty("uuid").toString();
                String type = rel.getType().name();
                Get get = new Get.Builder("relationship-index", uuid).type(type).build();
                JestResult result = esClient.execute(get);
                assertTrue(result.isSucceeded());
                tx.success();
            }
        }
    }

    //@Test
    public void testPropertyValuesTransformation() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration().withUuidProperty("uuid").with(IncludeAllRelationships.getInstance()), database));

        JsonFileMapping mapping = (JsonFileMapping) ServiceLoader.loadMapping("com.graphaware.module.es.mapping.JsonFileMapping");
        Map<String, String> mappingConfig = new HashMap<>();
        mappingConfig.put("file", "integration/mapping-transformation.json");

        configuration = ElasticSearchConfiguration.defaultConfiguration()
                .withMapping(mapping, mappingConfig)
                .with(IncludeAllRelationships.getInstance())
                .withUri(HOST)
                .withPort(PORT);

        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

        runtime.start();
        runtime.waitUntilStarted();
        database.execute("CREATE (l:Location {time:'1234', lat:'1.489', long:1234567890123})");
        TestUtil.waitFor(1500);
        try (Transaction tx = database.beginTx()) {
            for (Node node : database.getAllNodes()) {
                Get get = new Get.Builder("nodes", node.getProperty("uuid").toString()).type("locations").build();
                JestResult result = esClient.execute(get);
                assertTrue(result.isSucceeded());
                Map<String, Object> source = new HashMap<>();
                Map<String, Object> map = result.getSourceAsObject(source.getClass());
                assertEquals("1234567890123", map.get("longitude"));
                //assertEquals(1234, map.get("timestamp"));
                assertEquals(1.489, map.get("latitude"));
            }
            tx.success();
        }
    }

    public void testBasicJsonMappingReplicationWithQuery() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration().withUuidProperty("uuid").with(IncludeAllRelationships.getInstance()), database));

        JsonFileMapping mapping = (JsonFileMapping) ServiceLoader.loadMapping("com.graphaware.module.es.mapping.JsonFileMapping");
        Map<String, String> mappingConfig = new HashMap<>();
        mappingConfig.put("file", "integration/mapping-query.json");

        configuration = ElasticSearchConfiguration.defaultConfiguration()
                .withMapping(mapping, mappingConfig)
                .with(IncludeAllRelationships.getInstance())
                .withUri(HOST)
                .withPort(PORT);

        runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));
        runtime.start();
        runtime.waitUntilStarted();

        writeSomePersons();
        System.out.println("Finished writing...");
        TestUtil.waitFor(2000);
        verifyEsReplicationForNodeWithLabels("Person", mapping.getMappingRepresentation().getDefaults().getDefaultNodesIndex(), "persons", mapping.getMappingRepresentation().getDefaults().getKeyProperty());
        try (Transaction tx = database.beginTx()) {
            database.getAllRelationships().stream().forEach(r -> {
                new Neo4jElasticVerifier(database, configuration, esClient).verifyEsReplication(r, mapping.getMappingRepresentation().getDefaults().getDefaultRelationshipsIndex(), "workers", mapping.getKeyProperty());
            });
            tx.success();
        }
    }

    protected void verifyEsReplicationForNodeWithLabels(String label, String index, String type, String keyProperty) {
        try (Transaction tx = database.beginTx()) {
            database.findNodes(Label.label(label)).stream().forEach(n -> {
                new Neo4jElasticVerifier(database, configuration, esClient).verifyEsReplication(n, index, type, keyProperty);
            });
            tx.success();
        }
    }

    protected void verifyNoEsReplicationForNodesWithLabel(String label, String index, String type, String keyProperty) {
        try (Transaction tx = database.beginTx()) {
            database.findNodes(Label.label(label)).stream()
                    .forEach(n -> {
                        new Neo4jElasticVerifier(database, configuration, esClient).verifyNoEsReplication(n, index, type, keyProperty);
                    });
            tx.success();
        }
    }

    protected void writeSomePersons() {
        //tx 0
        database.execute("CREATE (n {name:'Hello'})");

        //tx1
        database.execute("CREATE (p:Person:Male {firstName:'Michal', lastName:'Bachman', age:30})-[:WORKS_FOR {since:2013, role:'MD'}]->(c:Company {name:'GraphAware', est: 2013})");

        //tx2
        database.execute("MATCH (ga:Company {name:'GraphAware'}) CREATE (p:Person:Male {firstName:'Adam', lastName:'George'})-[:WORKS_FOR {since:2014}]->(ga)");

        //tx3
        try (Transaction tx = database.beginTx()) {
            database.execute("MATCH (ga:Company {name:'GraphAware'}) CREATE (p:Person:Female {firstName:'Daniela', lastName:'Daniela'})-[:WORKS_FOR]->(ga)");
            database.execute("MATCH (p:Person {name:'Michal'}) SET p.age=31");
            database.execute("MATCH (p:Person {name:'Adam'})-[r]-() DELETE p,r");
            database.execute("MATCH (p:Person {name:'Michal'})-[r:WORKS_FOR]->() REMOVE r.role");
            tx.success();
        }
    }
    
    private void cleanUpData() {
        try (Transaction tx = database.beginTx()) {
            database.execute("MATCH ()-[r]-() DELETE r");
            database.execute("MATCH (p) DETACH DELETE p");
            tx.success();
        }
        
        DeleteByQuery delete = new DeleteByQuery.Builder("{ \"match_all\": {} }").addIndex("*")
            .addType("*")
            .build();
        esClient.execute(delete);
        database.shutdown();
        waitFor(1000);
    }


}
