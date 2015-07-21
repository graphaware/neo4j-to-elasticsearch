
package com.graphaware.module.es;

import com.graphaware.module.es.util.CustomClassLoading;
import com.graphaware.module.es.util.PassThroughProxyHandler;
import com.graphaware.module.es.wrapper.IGenericWrapper;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import com.graphaware.test.integration.NeoServerIntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.lang.reflect.Proxy;
import java.net.MalformedURLException;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModuleTest extends NeoServerIntegrationTest
{
  private static Logger LOG = LoggerFactory.getLogger(ModuleTest.class);
  private GraphDatabaseService database;
  private IGenericWrapper indexWrapper;

  @Before
  public void setUp()
  {
    final String classpath = System.getProperty("classpath");
    LOG.warn("classpath: " + classpath);
    Executors.newSingleThreadExecutor().execute(new Runnable()
    {
      @Override
      public void run()
      {
        try
        {
          CustomClassLoading loader = new CustomClassLoading(classpath);
          Class<Object> loadedClass = (Class<Object>) loader.loadClass("com.graphaware.module.es.wrapper.ESWrapper");
          indexWrapper = (IGenericWrapper) Proxy.newProxyInstance(this.getClass().getClassLoader(),
                  new Class[]
                  {
                    IGenericWrapper.class
                  },
                  new PassThroughProxyHandler(loadedClass.newInstance()));
          indexWrapper.startClient();
        }
        catch (MalformedURLException | ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException ex)
        {
        }
      }
    });

    database = new TestGraphDatabaseFactory().newImpermanentDatabase();
  }

  @After
  public void tearDown()
  {
    database.shutdown();
  }

  //@Ignore
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
