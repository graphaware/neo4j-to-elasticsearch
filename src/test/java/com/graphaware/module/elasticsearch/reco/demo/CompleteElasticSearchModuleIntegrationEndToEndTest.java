
package com.graphaware.module.elasticsearch.reco.demo;

import com.graphaware.integration.elasticsearch.wrapper.IGenericServerWrapper;
import com.graphaware.integration.elasticsearch.util.CustomClassLoading;
import com.graphaware.integration.elasticsearch.util.PassThroughProxyHandler;
import com.graphaware.module.elasticsearch.util.TestUtil;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.junit.Test;
import org.neo4j.graphdb.*;

import java.io.IOException;

import com.graphaware.test.data.DatabasePopulator;
import com.graphaware.test.data.GraphgenPopulator;
import com.graphaware.test.integration.GraphAwareApiTest;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import java.lang.reflect.Proxy;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

public class CompleteElasticSearchModuleIntegrationEndToEndTest
         extends GraphAwareApiTest
{

  private static final String ES_HOST = "localhost";
  private static final String ES_PORT = "9201";
  private static final String ES_CONN = String.format("http://%s:%s", ES_HOST, ES_PORT);
  private static final String ES_INDEX = "neo4jes";

  private IGenericServerWrapper embeddedServer;

  private static final Logger LOG = LoggerFactory.getLogger(CompleteElasticSearchModuleIntegrationEndToEndTest.class);

  //@Override
  protected String neo4jConfigFile()
  {
    return "neo4j-elasticsearch-reco.properties";
  }
  
  protected String propertiesFile() {
        return "src/test/resources/" + neo4jConfigFile();
    }

  //@Override
  protected String neo4jServerConfigFile()
  {
    return "neo4j-server-es.properties";
  }

  @Override
  public String baseUrl()
  {
    return "http://localhost:7575";
  }

  @Before
  public void setUp() throws IOException, InterruptedException, Exception
  {
    TestUtil.deleteDataDirectory();

    final String classpath = System.getProperty("classpath");
    LOG.warn("classpath: " + classpath);
    try
    {
      CustomClassLoading loader = new CustomClassLoading(classpath);
      Class<Object> loadedClass = (Class<Object>) loader.loadClass("com.graphaware.integration.elasticsearch.wrapper.ESServerWrapper");
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
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, InterruptedException, Exception
  {
    embeddedServer.stopEmbdeddedServer();
    super.tearDown();
    TestUtil.deleteDataDirectory();
  }

  protected DatabasePopulator databasePopulator()
  {
    return new GraphgenPopulator()
    {
      @Override
      protected String file() throws IOException
      {
        return new ClassPathResource("demo-data.cyp").getFile().getAbsolutePath();
      }
      
//      @Override
//      public void populate(GraphDatabaseService database)
//      {
//        String separator = separator();
//
//        String[] statementGroups = statementGroups();
//        if (statementGroups == null)
//          return;
//        
//        for (String statementGroup : statementGroups)
//            for (String statement : statementGroup.split(separator))
//              httpClient.executeCypher(baseUrl(), statement);
//      }
    };
  }

  protected void populateDatabase(GraphDatabaseService database)
  {
    DatabasePopulator populator = databasePopulator();
    if (populator != null)
    {
      populator.populate(database);
    }
  }

  @Test
  public void test() throws IOException
  {
    String executeCypher = httpClient.executeCypher(baseUrl(), "MATCH (p:Person {firstname:'Kelly', lastname:'Krajcik'}) return p");
    String response = httpClient.get(ES_CONN + "/" + ES_INDEX + "/Person/_search?q=firstname:Kelly", HttpStatus.OK_200);
    
//    JestClientFactory factory = new JestClientFactory();
//
//    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_CONN)
//            .multiThreaded(true)
//            .build());
//    JestClient client = factory.getObject();
//    
//    String query = "{\n" +
//"  \"bool\" : {\n" +
//"    \"must\" : {\n" +
//"      \"match_all\" : { }\n" +
//"    },\n" +
//"    \"should\" : {\n" +
//"      \"match\" : {\n" +
//"        \"__forUser\" : {\n" +
//"          \"query\" : \"1000\",\n" +
//"          \"type\" : \"boolean\"\n" +
//"        }\n" +
//"      }\n" +
//"    }\n" +
//"  }\n" +
//"}";
//    
//    Search search = new Search.Builder(query)
//                // multiple index or types can be added.
//                .addIndex(ES_INDEX)
//                .addType("Person")
//                .build();
//
//
//    SearchResult result = client.execute(search);
    
    //String response = httpClient.get(ES_CONN + "/" + ES_INDEX + "/Company/_search?q=firstname:Kelly", HttpStatus.OK_200);
    String result1 = httpClient.get(baseUrl() + "/graphaware/recommendation/filter/Durgan%20LLC?limit=10&ids=148,197,27,4,5,6,7,8,9", HttpStatus.OK_200);
        
    boolean res = response.contains("total\": 1");
    //assertEquals(res, true);

//    Get get = new Get.Builder(ES_INDEX, nodeId).type(car.name()).build();
//    JestResult result = null;
//
//    try
//    {
//      result = client.execute(get);
//    }
//    catch (IOException e)
//    {
//      e.printStackTrace();
//    }
//
//    notNull(result);
//    isTrue(result.isSucceeded());
  }
}
