/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.graphaware.module.es.wrapper;

import java.util.List;
import java.util.Map;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import static org.elasticsearch.node.NodeBuilder.*;
import org.elasticsearch.index.get.GetField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ale
 */
public class ESWrapper implements IGenericWrapper
{
  private static final Logger LOG = LoggerFactory.getLogger(ESWrapper.class);

  private Node node;
  private Client client;

  public ESWrapper()
  {

  }

  public void startLocalClient()
  {
    try
    {
      node = nodeBuilder().local(true)
              .settings(ImmutableSettings.builder()
              .put("script.engine.groovy.inline.aggs", "on")
              .put("script.inline", "on")
              .put("script.indexed", "on"))
              .node();
      client = node.client();
    }
    catch (Exception e)
    {
      LOG.error("Error while starting it up", e);
    }
  }

  public void startClient(String clustername, boolean clientNode)
  {
    try
    {
      node = nodeBuilder()
              .clusterName(clustername)
              .client(clientNode)
              .settings(ImmutableSettings.builder()
              .put("script.engine.groovy.inline.aggs", "on")
              .put("script.inline", "on")
              .put("script.indexed", "on")).node();
      client = node.client();
    }
    catch (Exception e)
    {
      LOG.error("Error while starting it up", e);
    }
  }

  public void stopClient()
  {
    node.close();
  }

  public void add(final String indexName, final String type, final long nodeId, final Map<String, String> propertiesValue)
  {
    //LOG.warn("ADD: " + nodeId + " -> " + propertiesValue.size());
    try
    {
      String node = String.valueOf(nodeId);

      IndexRequest indexRequest = new IndexRequest(indexName, type, node)
              .source(propertiesValue);
//      UpdateRequest updateRequest = new UpdateRequest(indexName, node, propertyValue.toString())
//              .addScriptParam("newNodeId", nodeId)
//              .script("ctx._source.nodes += newNodeId")
//              .upsert(indexRequest);
      //UpdateResponse result = client.update(updateRequest).get();
      ActionFuture<IndexResponse> result = client.index(indexRequest);
      //LOG.warn("result: " + result);
//      GetField field = result.getGetResult().field("nodes");
//      List<Object> values = field.getValues();
//      if (values != null && values.size() > 0)
//      {
//        logger.warning("Lookup result : " + values.size());
//        for (Object value : values)
//          logger.warning(">>>>>>>>: " + value);
//      }
    }
    catch (Exception ex)
    {
      //LOG.error("Error while upserting", ex);
    }
  }
  @Override
  public long[] lookup(long indexId, String indexName, Object propertyValue)
  {
    //LOG.warn("lookup: " + indexId + " " + indexName + " " + propertyValue);
    GetResponse response = client.prepareGet(indexName, String.valueOf(indexId), (String) propertyValue)
            .execute()
            .actionGet();
    GetField field = response.getField("nodes");
    List<Object> values = field.getValues();
    long[] result = null;
    int i = 0;
    if (values != null && values.size() > 0)
    {
      result = new long[values.size()];
      for (Object value : values)
      {
        result[i++] = ((Long) value).longValue();
      }
    }
    return result == null ? null : result;
  }
}
