
package com.graphaware.module.es;

import com.graphaware.module.es.util.CustomClassLoading;
import com.graphaware.module.es.util.PassThroughProxyHandler;
import com.graphaware.module.es.wrapper.IGenericServerWrapper;
import com.graphaware.test.integration.NeoServerIntegrationTest;
import com.graphaware.test.util.TestHttpClient;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.assertEquals;
import org.neo4j.shell.util.json.JSONObject;

public class EsModuleInitializationTest extends NeoServerIntegrationTest
{

  private static final Logger LOG = LoggerFactory.getLogger(EsModuleInitializationTest.class);

  private static String ELASTICSEARCH_URL = "http://localhost:9200";
  private IGenericServerWrapper embeddedServer;
  
  private String neo4jConfigFile;
  private String neo4jConfigServerFile;
  
  

  @Override
  protected String neo4jConfigFile()
  {
    return neo4jConfigFile ;
  }
  
  @Override
   protected String neo4jServerConfigFile() {
        return neo4jConfigServerFile;
    }

  protected String baseUrl()
  {
    return "http://localhost:7474";
  }

  @Override
  public void setUp() throws IOException, InterruptedException
  {
    TestUtil.deleteDataDirectory();

  }

  @Override
  public void tearDown() throws IOException, InterruptedException
  {

    TestUtil.deleteDataDirectory();
  }

  @Test
  public void testNewNode() throws InterruptedException, IOException
  {
    neo4jConfigFile = "neo4j.properties";
    neo4jConfigServerFile = "neo4j-server.properties";
    super.setUp();
    
    httpClient.executeCypher(baseUrl(), "CREATE (c:Car {name:'Tesla Model S'})");
    httpClient.executeCypher(baseUrl(), "CREATE (c:Car {name:'Ferrari 458'})");
    httpClient.executeCypher(baseUrl(), "CREATE (c:Car {name:'Maserati Ghibli'})");

    super.tearDown();
    
    final String classpath = System.getProperty("classpath");
    LOG.warn("classpath: " + classpath);
    try
    {
      CustomClassLoading loader = new CustomClassLoading(classpath);
      Class<Object> loadedClass = (Class<Object>) loader.loadClass("com.graphaware.module.es.wrapper.ESServerWrapper");
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
    
    neo4jConfigFile = "neo4j-es.properties";
    neo4jConfigServerFile = "neo4j-server-es.properties";
    
    TestHttpClient tmpHttpClient = createHttpClient();
    String response = tmpHttpClient.get(ELASTICSEARCH_URL + "/neo4jes/node/0", HttpStatus.NOT_FOUND_404);
    boolean res = response.contains("IndexMissingException");
    assertEquals(res, true);
    
    super.setUp();
    
    response = httpClient.get(ELASTICSEARCH_URL + "/neo4jes/node/0", HttpStatus.OK_200);
     res = response.contains("\"found\":true");
    assertEquals(res, true);
    
    response = httpClient.get(ELASTICSEARCH_URL + "/neo4jes/node/1", HttpStatus.OK_200);
    res = response.contains("\"found\":true");
    assertEquals(res, true);
    
    response = httpClient.get(ELASTICSEARCH_URL + "/neo4jes/node/2", HttpStatus.OK_200);
    res = response.contains("\"found\":true");
    assertEquals(res, true);
    
    httpClient.executeCypher(baseUrl(), "MATCH (c:Car {name:'Tesla Model S'}) DELETE c");
    response = httpClient.get(ELASTICSEARCH_URL + "/neo4jes/node/0", HttpStatus.NOT_FOUND_404);
    res = response.contains("\"found\":false");
    assertEquals(res, true);

    httpClient.executeCypher(baseUrl(), "MATCH (c:Car {name:'Maserati Ghibli'}) set c.name = 'Tesla Model S' ");
    //Wait for complete index propagation
    TimeUnit.SECONDS.sleep(3);
    response = httpClient.get(ELASTICSEARCH_URL + "/neo4jes/node/_search?q=nodeId:2", HttpStatus.OK_200);
    res = response.contains("Tesla");
    assertEquals(res, true);

    super.tearDown();
    embeddedServer.stopEmbdeddedServer();
  }
  
//  @Test
//  public void testDelete()
//  {
//    /*
//     * check nodes status
//     * http://localhost:9200/_cat/indices?v
//     */
//    String executeCypher = httpClient.executeCypher(baseUrl(), "MATCH (c:Car {name:'Tesla Model S'}) DELETE c");
//
//    //String response = httpClient.get(ELASTICSEARCH_URL + "/_cluster/health?pretty=true", HttpStatus.OK_200);
//    String response = httpClient.get(ELASTICSEARCH_URL + "/neo4jes/node/0", HttpStatus.NOT_FOUND_404);
////    boolean res = response.contains("\"found\":false");
////    assertEquals(res, true);
//  }

}
