
package com.graphaware.module.es;

import com.graphaware.module.es.util.CustomClassLoading;
import com.graphaware.module.es.util.PassThroughProxyHandler;
import com.graphaware.module.es.wrapper.IGenericServerWrapper;
import com.graphaware.test.integration.NeoServerIntegrationTest;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.assertEquals;
import org.neo4j.shell.util.json.JSONObject;

public class EsModuleEndToEndTest extends NeoServerIntegrationTest
{

  private static final Logger LOG = LoggerFactory.getLogger(EsModuleEndToEndTest.class);

  private static String ELASTICSEARCH_URL = "http://localhost:9200";
  private IGenericServerWrapper embeddedServer;

  @Override
  protected String neo4jConfigFile()
  {
    return "neo4j-es.properties";
  }

  protected String baseUrl()
  {
    return "http://localhost:7474";
  }

  @Override
  public void setUp() throws IOException, InterruptedException
  {
    TestUtil.deleteDataDirectory();
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

    super.setUp();
  }

  @Override
  public void tearDown() throws IOException, InterruptedException
  {
    super.tearDown();
    embeddedServer.stopEmbdeddedServer();
    TestUtil.deleteDataDirectory();
  }

  @Test
  public void testNewNode() throws InterruptedException
  {
    /*
     * check nodes status
     * http://localhost:9200/_cat/indices?v
     * http://localhost:9200/_cluster/health?pretty=true
     *    
     */
    String executeCypher = httpClient.executeCypher(baseUrl(), "CREATE (c:Car {name:'Tesla Model S'})");
    String response = httpClient.get(ELASTICSEARCH_URL + "/neo4jes/node/0", HttpStatus.OK_200);
    boolean res = response.contains("\"found\":true");
    assertEquals(res, true);
    
    executeCypher = httpClient.executeCypher(baseUrl(), "CREATE (c:Car {name:'Ferrari 458'})");
    response = httpClient.get(ELASTICSEARCH_URL + "/neo4jes/node/1", HttpStatus.OK_200);
    res = response.contains("\"found\":true");
    assertEquals(res, true);
    
    executeCypher = httpClient.executeCypher(baseUrl(), "CREATE (c:Car {name:'Maserati Ghibli'})");
    response = httpClient.get(ELASTICSEARCH_URL + "/neo4jes/node/2", HttpStatus.OK_200);
    res = response.contains("\"found\":true");
    assertEquals(res, true);
    
    executeCypher = httpClient.executeCypher(baseUrl(), "MATCH (c:Car {name:'Tesla Model S'}) DELETE c");
    response = httpClient.get(ELASTICSEARCH_URL + "/neo4jes/node/0", HttpStatus.NOT_FOUND_404);
    res = response.contains("\"found\":false");
    assertEquals(res, true);

    executeCypher = httpClient.executeCypher(baseUrl(), "MATCH (c:Car {name:'Maserati Ghibli'}) set c.name = 'Tesla Model S' ");
    //Wait for complete index propagation
    TimeUnit.SECONDS.sleep(3);
    response = httpClient.get(ELASTICSEARCH_URL + "/neo4jes/node/_search?q=nodeId:2", HttpStatus.OK_200);
    res = response.contains("Tesla");
    assertEquals(res, true);

    
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
