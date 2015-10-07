
package com.graphaware.elasticsearch;

import com.google.gson.JsonObject;
import com.graphaware.elasticsearch.reco.demo.JestPersonResult;
import com.graphaware.elasticsearch.wrapper.IGenericServerWrapper;
import com.graphaware.elasticsearch.util.CustomClassLoading;
import com.graphaware.elasticsearch.util.PassThroughProxyHandler;
import com.graphaware.runtime.RuntimeRegistry;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Get;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.apache.commons.lang.Validate;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.IOException;

import java.lang.reflect.Proxy;
import java.util.List;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.Assert;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.neo4j.tooling.GlobalGraphOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchModuleIntegrationTest
{

    private static final String ES_HOST = "localhost";
    private static final String ES_PORT = "9201";
    private static final String ES_CONN = String.format("http://%s:%s", ES_HOST, ES_PORT);
    private static final String ES_INDEX = "neo4jes";
    private final String UUID = "uuid";
    private IGenericServerWrapper embeddedServer;

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchModuleIntegrationTest.class);

    @Before
    public void setUp()
    {
        final String classpath = System.getProperty("classpath");
        LOG.warn("classpath: " + classpath);
        try
        {
            CustomClassLoading loader = new CustomClassLoading(classpath);
            Class<Object> loadedClass = (Class<Object>) loader.loadClass("com.graphaware.elasticsearch.wrapper.ESServerWrapper");
            embeddedServer = (IGenericServerWrapper) Proxy.newProxyInstance(this.getClass().getClassLoader(),
                    new Class[]
                    {
                      IGenericServerWrapper.class
                    },
                    new PassThroughProxyHandler(loadedClass.newInstance()));
            embeddedServer.startEmbdeddedServer();
        }
        catch (Exception ex)
        {
            LOG.warn("Error while creating and starting client", ex);
        }
    }

    @After
    public void tearDown() {
        embeddedServer.stopEmbdeddedServer();
    }

    @Test
    public void test() throws IOException
    {
      GraphDatabaseService database = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
              .loadPropertiesFromFile(this.getClass().getClassLoader().getResource("neo4j-elasticsearch.properties").getPath())
              .newGraphDatabase();

      RuntimeRegistry.getRuntime(database).waitUntilStarted();

      final Label label = DynamicLabel.label("CAR");
      String nodeId = testNewNode(database, label);
      testUpdateNode(database, label, nodeId);
      testDeleteNode(database, label, nodeId);
      final Label label2 = DynamicLabel.label("CAR2");
      stressTest(database, 1000, label, label2);

      database.shutdown();
    }

    private String testNewNode(GraphDatabaseService database, final Label label)
    {
      String nodeId = null;
      try (Transaction tx = database.beginTx())
      {
        Node node = database.createNode(label);
        node.setProperty("name", "Model S");
        node.setProperty("manufacturer", "Tesla");
        tx.success();
      }

      try (Transaction tx = database.beginTx())
      {
        for (Node node : GlobalGraphOperations.at(database).getAllNodesWithLabel(label))
        {
          assertTrue(node.hasProperty(UUID));
          nodeId = String.valueOf(node.getProperty(UUID));
          break;
        }
        tx.success();
      }

      try
      {
        Thread.sleep(1000);
      }
      catch (InterruptedException e)
      {
        //ok
      }

      JestClientFactory factory = new JestClientFactory();
      factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_CONN)
              .multiThreaded(true)
              .build());
      JestClient client = factory.getObject();
      Get get = new Get.Builder(ES_INDEX, nodeId).type(label.name()).build();
      JestResult result = null;
      try
      {
        result = client.execute(get);
      }
      catch (IOException e)
      {
        e.printStackTrace();
      }
      Validate.notNull(result);
      Validate.isTrue(result.isSucceeded());
      return nodeId;
    }

    private void testUpdateNode(GraphDatabaseService database, Label label, String nodeId)
    {
      try (Transaction tx = database.beginTx())
      {
        database.getNodeById(0).setProperty("newProp", "newPropValue");
        tx.success();
      }

      try
      {
        Thread.sleep(1000);
      }
      catch (InterruptedException e)
      {
        //ok
      }

      JestClientFactory factory = new JestClientFactory();
      factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_CONN)
              .multiThreaded(true)
              .build());
      JestClient client = factory.getObject();
      Get get = new Get.Builder(ES_INDEX, nodeId).type(label.name()).build();
      JestResult result = null;
      try
      {
        result = client.execute(get);
      }
      catch (IOException e)
      {
        e.printStackTrace();
      }
      Validate.notNull(result);
      Validate.isTrue(result.isSucceeded());
      Validate.isTrue(((JsonObject) (result.getJsonObject().get("_source"))).get("newProp") != null);
    }

    private void testDeleteNode(GraphDatabaseService database, Label label, String nodeId)
    {
      try (Transaction tx = database.beginTx())
      {
        database.getNodeById(0).delete();
        tx.success();
      }

      try
      {
        Thread.sleep(1000);
      }
      catch (InterruptedException e)
      {
        //ok
      }

      JestClientFactory factory = new JestClientFactory();
      factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_CONN)
              .multiThreaded(true)
              .build());
      JestClient client = factory.getObject();
      Get get = new Get.Builder(ES_INDEX, nodeId).type(label.name()).build();
      JestResult result = null;
      try
      {
        result = client.execute(get);
      }
      catch (IOException e)
      {
        e.printStackTrace();
      }
      Validate.notNull(result);
      Validate.isTrue(!result.getJsonObject().get("found").getAsBoolean());
    }

    private void stressTest(GraphDatabaseService database, int maxNodes, Label... labels) throws IOException
    {
      String nodeId = null;
      try (Transaction tx = database.beginTx())
      {
        for (int i = 0; i < maxNodes; i++)
        {
          Node node = database.createNode(labels);
          node.setProperty("name", "Model_" + i);
          node.setProperty("manufacturer", "Tesla_ " + i);
        }

        tx.success();
      }
      try
      {
        Thread.sleep(10000);
      }
      catch (InterruptedException e)
      {
        //ok
      }

      JestClientFactory factory = new JestClientFactory();
      factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_CONN)
              .multiThreaded(true)
              .build());
      JestClient client = factory.getObject();
      int k = 1;
      for (Label label : labels)
        try (Transaction tx = database.beginTx())
        {
          for (Node node : GlobalGraphOperations.at(database).getAllNodesWithLabel(label))
          {
            assertTrue(node.hasProperty(UUID));
            nodeId = String.valueOf(node.getProperty(UUID));
            Get getRequest = new Get.Builder(ES_INDEX, nodeId).type(label.name()).build();

            try
            {
              JestResult result = null;
              result = client.execute(getRequest);
              Validate.notNull(result);
              Validate.isTrue(result.isSucceeded());
              JestUUIDResult hit = result.getSourceAsObject(JestUUIDResult.class);
              Validate.isTrue(hit.getDocumentId().equalsIgnoreCase(nodeId));
            }
            catch (IOException e)
            {
              e.printStackTrace();
            }
            if (k++ % 100 == 0)
              LOG.warn("Tested: " + (k - 1) + " nodes");
          }
          tx.success();
        }

      {
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
        for (Label label : labels)
        {
          Search search = new Search.Builder(query).addIndex(ES_INDEX)
                  .addType(label.name())
                  .setParameter("size", maxNodes)
                  .build();
          SearchResult result = client.execute(search);
          List<SearchResult.Hit<JestUUIDResult, Void>> hits = result.getHits(JestUUIDResult.class);
          Assert.assertEquals(maxNodes, hits.size());
        }
      }
    }
}
