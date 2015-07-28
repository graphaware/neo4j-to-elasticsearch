
package com.graphaware.module.es;

import com.graphaware.module.es.util.CustomClassLoading;
import com.graphaware.module.es.util.PassThroughProxyHandler;
import com.graphaware.module.es.wrapper.IGenericClientWrapper;
import com.graphaware.module.es.wrapper.IGenericServerWrapper;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import com.graphaware.test.integration.NeoServerIntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.lang.reflect.Proxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModuleTest extends NeoServerIntegrationTest
{
  private static Logger LOG = LoggerFactory.getLogger(ModuleTest.class);
  private GraphDatabaseService database;
  private IGenericClientWrapper indexWrapper;

  @Override
  protected String neo4jConfigFile()
  {
    return "neo4j-es.properties";
  }

  @Override
  protected String neo4jServerConfigFile()
  {
    return "neo4j-server-es.properties";
  }

  @Before
  public void setUp()
  {
    final String classpath = System.getProperty("classpath");
    LOG.warn("classpath: " + classpath);
    TestUtil.deleteDataDirectory();
    try
    {
      CustomClassLoading loader = new CustomClassLoading(classpath);
      Class<Object> loadedClass = (Class<Object>) loader.loadClass("com.graphaware.module.es.wrapper.ESClientWrapper");
      indexWrapper = (IGenericClientWrapper) Proxy.newProxyInstance(this.getClass().getClassLoader(),
              new Class[]
              {
                IGenericClientWrapper.class
              },
              new PassThroughProxyHandler(loadedClass.newInstance()));
      indexWrapper.startLocalClient();
    }
    catch (Exception ex)
    {
      LOG.warn("Error while creating and starting client", ex);
    }

    database = new TestGraphDatabaseFactory().newImpermanentDatabase();
  }

  @After
  public void tearDown()
  {
    database.shutdown();
    TestUtil.deleteDataDirectory();
  }

  @Test
  public void moduleShouldInitializeCorrectly()
  {
    final EsConfiguration conf = EsConfiguration.defaultConfiguration().withIndexName("neo4j");
    GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);

    EsModule module = new EsModule("es", conf, database, indexWrapper);

    runtime.registerModule(module);

    runtime.start();
  }
}
