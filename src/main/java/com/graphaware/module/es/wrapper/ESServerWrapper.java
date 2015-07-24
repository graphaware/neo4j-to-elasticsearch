/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.graphaware.module.es.wrapper;

import java.util.concurrent.Executors;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import static org.elasticsearch.node.NodeBuilder.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ale
 */
public class ESServerWrapper implements IGenericServerWrapper
{
  public static final String DEFAULT_DATA_DIRECTORY = "target/elasticsearch-data";
  private static final Logger LOG = LoggerFactory.getLogger(ESServerWrapper.class);
  private Node embeddedNode;

  public ESServerWrapper()
  {

  }

  public void startEmbdeddedServer()
  {
    final ClassLoader currentClassLoader = this.getClass().getClassLoader();
    
    Executors.newSingleThreadExecutor().execute(new Runnable()
    {
      @Override
      public void run()
      {
        try
        {
          Thread.currentThread().setContextClassLoader(currentClassLoader);
          embeddedNode = nodeBuilder().local(false)
                  .settings(ImmutableSettings.builder()
                          .put("transport.tcp.port", "9300")
                          .put("script.engine.groovy.inline.aggs", "on")
                          .put("script.inline", "on")
                          .put("script.indexed", "on")
                          .put("cluster.name", "neo4j-elasticsearch"))
                  .node();
          embeddedNode.start();
        }
        catch (Exception e)
        {
          LOG.error("Error while starting ES embedded server up ...", e);
        }
      }
    });

  }
  @Override
  public void stopEmbdeddedServer()
  {
    embeddedNode.stop();
  }
}
