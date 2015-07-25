/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.graphaware.module.es.wrapper;

import com.esotericsoftware.minlog.Log;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
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
    final CountDownLatch done = new CountDownLatch(1);
    final ExecutorService executor = Executors.newSingleThreadExecutor();

    executor.execute(new Runnable()
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
          Log.warn("Embedded ElasticSearch started ...");
          done.countDown();
        }
        catch (Exception e)
        {
          LOG.error("Error while starting ES embedded server up ...", e);
        }
      }
    });
    try
    {
      Log.warn("Waiting for embedded startup completion ...");
      done.await(20, TimeUnit.SECONDS);
      Log.warn("... time is up!");
    }
    catch (InterruptedException ex)
    {
      Log.error("Error while waiting");
    }

  }
  @Override
  public void stopEmbdeddedServer()
  {
    embeddedNode.stop();
  }
}
