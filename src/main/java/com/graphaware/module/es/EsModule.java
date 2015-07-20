/*
 * Copyright (c) 2014 GraphAware
 *
 * This file is part of GraphAware.
 *
 * GraphAware is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.es;

import com.graphaware.module.es.util.CustomClassLoading;
import com.graphaware.module.es.wrapper.IGenericWrapper;
import com.graphaware.runtime.module.BaseTxDrivenModule;
import com.graphaware.runtime.module.DeliberateTransactionRollbackException;
import com.graphaware.tx.event.improved.api.ImprovedTransactionData;
import com.graphaware.module.es.util.PassThroughProxyHandler;
import java.lang.reflect.Proxy;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link com.graphaware.runtime.module.TxDrivenModule} that assigns UUID's to
 * nodes in the graph.
 */
public class EsModule extends BaseTxDrivenModule<Void>
{

  private static final Logger logger = LoggerFactory.getLogger(EsModule.class);

  private final EsConfiguration esConfiguration;
  private final GraphDatabaseService database;
  private IGenericWrapper indexWrapper;

  /**
   * Construct a new UUID module.
   *
   * @param moduleId ID of the module.
   */
  public EsModule(String moduleId, EsConfiguration configuration, GraphDatabaseService database)
  {
    super(moduleId);
    this.esConfiguration = configuration;
    this.database = database;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EsConfiguration getConfiguration()
  {
    return esConfiguration;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void initialize(GraphDatabaseService database)
  {

    Executors.newSingleThreadExecutor().execute(new Runnable()
    {
      @Override
      public void run()
      {
        try
        {
          CustomClassLoading loader = new CustomClassLoading(esConfiguration.getClasspathDirectory());
          Class<Object> loadedClass = (Class<Object>) loader.loadClass("com.graphaware.module.es.wrapper.ESWrapper");
          indexWrapper = (IGenericWrapper) Proxy.newProxyInstance(this.getClass().getClassLoader(),
                  new Class[]
                  {
                    IGenericWrapper.class
                  },
                  new PassThroughProxyHandler(loadedClass.newInstance()));
          indexWrapper.startClient();
          logger.warn("Client client = node.client();");
        }
        catch (MalformedURLException | ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException ex)
        {
          logger.error("Error while starting node", ex);
        }
      }
    });
    //Preload everything
//        new IterableInputBatchTransactionExecutor<>(
//                database,
//                BATCH_SIZE,
//                new AllNodes(database, BATCH_SIZE),
//                new UnitOfWork<Node>() {
//                    @Override
//                    public void execute(GraphDatabaseService database, Node node, int batchNumber, int stepNumber) {
//                        if (getConfiguration().getInclusionPolicies().getNodeInclusionPolicy().include(node)) {
//                            assignUuid(node);
//                        }
//                    }
//                }
//        ).execute();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Void beforeCommit(ImprovedTransactionData transactionData) throws DeliberateTransactionRollbackException
  {

    //Set the UUID on all created nodes
    for (Node node : transactionData.getAllCreatedNodes())
    {
      Map<String, String> properties = new HashMap<>();
      Iterable<String> propertyKeys = node.getPropertyKeys();
      for (String key : propertyKeys)
        properties.put(key, (String) node.getProperty(key));
      indexWrapper.add(esConfiguration.getIndexName(), "nodes", node.getId(), properties);
    }
//
//        for (Node node : transactionData.getAllDeletedNodes()) {
//            uuidIndexer.deleteNodeFromIndex(node);
//        }
//
//        //Check if the UUID has been modified or removed from the node and throw an error
//        for (Change<Node> change : transactionData.getAllChangedNodes()) {
//            if (!change.getCurrent().hasProperty(esConfiguration.getUuidProperty())) {
//                throw new DeliberateTransactionRollbackException("You are not allowed to remove the " + esConfiguration.getUuidProperty() + " property");
//            }
//
//            if (!change.getPrevious().getProperty(esConfiguration.getUuidProperty()).equals(change.getCurrent().getProperty(esConfiguration.getUuidProperty()))) {
//                throw new DeliberateTransactionRollbackException("You are not allowed to modify the " + esConfiguration.getUuidProperty() + " property");
//            }
//        }
    return null;
  }
}
