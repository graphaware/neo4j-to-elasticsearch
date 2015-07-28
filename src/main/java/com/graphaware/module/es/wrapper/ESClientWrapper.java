/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.graphaware.module.es.wrapper;

import com.esotericsoftware.minlog.Log;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
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
  public void startClient(String clustername, List<InetSocketAddress> clusterURLs)
  {
    try
    {
      Settings settings = ImmutableSettings.settingsBuilder()
              .put("cluster.name", clustername).build();
      client = new TransportClient(settings);
      for (InetSocketAddress address : clusterURLs)
          ((TransportClient)client).addTransportAddress(new InetSocketTransportAddress(address));
      //new InetSocketTransportAddress("localhost", 9300)
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

  public void add(final String indexName, final String type, final long nodeId, final Map<String, String> propertiesValue, boolean upsert)
  {
    try
    {
      String nodeValue = String.valueOf(nodeId);
      XContentBuilder builder = jsonBuilder().startObject();
      builder.field("nodeId", nodeValue);

      for (String propertyKey : propertiesValue.keySet())
        builder.field(propertyKey, propertiesValue.get(propertyKey));
      builder.endObject();

      if (!upsert)
        insert(indexName, type, nodeValue, builder);
      else
        upsert(indexName, type, nodeValue, builder);
    }
    catch (Exception ex)
    {
      LOG.error("Error while inserting", ex);
      //here we should do something rise error (but this will cause issue in the transaction)
    }
  }
  private void upsert(final String indexName, final String type, String nodeValue, XContentBuilder builder) throws InterruptedException, ExecutionException
  {
    IndexRequest indexRequest = new IndexRequest(indexName, type, nodeValue)
            .source(builder);
    UpdateRequest updateRequest = new UpdateRequest(indexName, type, nodeValue)
            .doc(builder)
            .upsert(indexRequest);
    UpdateResponse response = client.update(updateRequest).get();
    String _index = response.getIndex();
    String _type = response.getType();
    String _id = response.getId();
    long _version = response.getVersion();
    boolean created = response.isCreated();
    LOG.warn("index " + _index + " type: " + _type + " id: " + _id + " version: " + _version + " created: " + created);
  }
  private void insert(final String indexName, final String type, String nodeValue, XContentBuilder builder) throws ElasticsearchException
  {
    IndexResponse response = client.prepareIndex(indexName, type, nodeValue)
            .setSource(builder)
            .execute()
            .actionGet();
    String _index = response.getIndex();
    String _type = response.getType();
    String _id = response.getId();
    long _version = response.getVersion();
    boolean created = response.isCreated();
    LOG.warn("index " + _index + " type: " + _type + " id: " + _id + " version: " + _version + " created: " + created);
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
        result[i++] = ((Long) value);
      }
    }
    return result == null ? null : result;
  }
  @Override
  public void delete(String indexName, String type, long nodeId)
  {
    String nodeidValue = String.valueOf(nodeId);
    DeleteResponse response = client.prepareDelete(indexName, type, nodeidValue)
            .execute()
            .actionGet();
    LOG.warn("Delete result: " + response.isFound());
  }
  @Override
  public void update(String indexName, String type, long nodeId, Map<String, String> propertiesValue)
  {
    try
    {
      String nodeValue = String.valueOf(nodeId);
      XContentBuilder builder = jsonBuilder().startObject();
      builder.field("nodeId", nodeValue);

      for (String propertyKey : propertiesValue.keySet())
        builder.field(propertyKey, propertiesValue.get(propertyKey));
      builder.endObject();

      UpdateRequest updateRequest = new UpdateRequest();
      updateRequest.index(indexName);
      updateRequest.type(type);
      updateRequest.id(nodeValue);
      updateRequest.doc(builder);
      UpdateResponse updateResponse = client.update(updateRequest).get();
      Log.warn("Update Result: " + updateResponse);
    }
    catch (Exception ex)
    {
      LOG.warn("Error while updating: " + nodeId, ex);
    }
  }
}
