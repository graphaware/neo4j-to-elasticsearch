/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.graphaware.module.es.wrapper;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import org.elasticsearch.node.Node;
import static org.elasticsearch.node.NodeBuilder.*;
import org.elasticsearch.index.get.GetField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ale
 */
public class ESClientWrapper implements IGenericClientWrapper
{
  public static final String DEFAULT_DATA_DIRECTORY = "target/elasticsearch-data";
  private static final Logger LOG = LoggerFactory.getLogger(ESClientWrapper.class);

  private Node node;
  private Client client;

  public ESClientWrapper()
  {

  }

  @Override
  public void startLocalClient()
  {
    
    LOG.warn("ClassLoader: " + this.getClass().getClassLoader());
    try
    {
      node = nodeBuilder().local(true)
              .settings(ImmutableSettings.builder()
                      .put("script.engine.groovy.inline.aggs", "on")
                      .put("script.inline", "on")
                      .put("script.indexed", "on")
                      .put("path.data", DEFAULT_DATA_DIRECTORY))
              .node();
      client = node.client();
    }
    catch (Exception e)
    {
      LOG.error("Error while starting it up", e);
    }
  }

  @Override
  public void startClient(String clustername, boolean clientNode)
  {
    try
    {
      Settings settings = ImmutableSettings.settingsBuilder()
              .put("cluster.name", clustername).build();
      client = new TransportClient(settings)
              .addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
    }
    catch (Exception e)
    {
      LOG.error("Error while starting it up", e);
    }
  }

  public void stopClient()
  {
    if (client != null)
      client.close();
    if (node != null)
      node.close();
  }

  public void add(final String indexName, final String type, final long nodeId, final Map<String, String> propertiesValue)
  {
    try
    {
      String nodeValue = String.valueOf(nodeId);
      XContentBuilder builder = jsonBuilder().startObject();
      builder.field("nodeId", nodeValue);

      for (String propertyKey : propertiesValue.keySet())
        builder.field(propertyKey, propertiesValue.get(propertyKey));
      builder.endObject();

      IndexResponse response = client.prepareIndex(indexName, type, nodeValue)
              .setSource(builder)
              .execute()
              .actionGet();
      // Index name
      String _index = response.getIndex();
// Type name
      String _type = response.getType();
// Document ID (generated or not)
      String _id = response.getId();
// Version (if it's the first time you index this document, you will get: 1)
      long _version = response.getVersion();
// isCreated() is true if the document is a new one, false if it has been updated
      boolean created = response.isCreated();
      LOG.warn("index " + _index + " type: " + _type + " id: " + _id + " version: " + _version + " created: " + created);
//      UpdateRequest updateRequest = new UpdateRequest(indexName, node, propertyValue.toString())
//              .addScriptParam("newNodeId", nodeId)
//              .script("ctx._source.nodes += newNodeId")
//              .upsert(indexRequest);
      //UpdateResponse result = client.update(updateRequest).get();
      //ActionFuture<IndexResponse> result = client.index(indexRequest);
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
      LOG.error("Error while upserting", ex);
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
