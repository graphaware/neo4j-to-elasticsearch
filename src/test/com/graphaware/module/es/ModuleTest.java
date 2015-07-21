package com.graphaware.module.es;

import com.graphaware.module.es.util.CustomClassLoading;
import com.graphaware.module.es.util.PassThroughProxyHandler;
import com.graphaware.module.es.wrapper.IGenericWrapper;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.lang.reflect.Proxy;
import java.net.MalformedURLException;
import java.util.concurrent.Executors;

public class ModuleTest {
    private GraphDatabaseService database;

    @Before
    public void setUp() {
        Executors.newSingleThreadExecutor().execute(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    CustomClassLoading loader = new CustomClassLoading("/Users/MicTech/GraphAware/neo4j-es/target/");
                    Class<Object> loadedClass = (Class<Object>) loader.loadClass("com.graphaware.module.es.wrapper.ESWrapper");
                    IGenericWrapper indexWrapper = (IGenericWrapper) Proxy.newProxyInstance(this.getClass().getClassLoader(),
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
    public void tearDown() {
        database.shutdown();
    }

    @Ignore
    @Test
    public void moduleShouldInitializeCorrectly() {
        final EsConfiguration conf = EsConfiguration.defaultConfiguration().withIndexName("neo4j");
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);

        EsModule module = new EsModule("es", conf, database);

        runtime.registerModule(module);

        runtime.start();
    }
}
