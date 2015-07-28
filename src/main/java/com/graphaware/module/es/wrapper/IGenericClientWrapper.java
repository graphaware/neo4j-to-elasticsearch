
package com.graphaware.module.es.wrapper;

import java.util.Map;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author ale
 */
public interface IGenericClientWrapper
{
  public void startLocalClient();
  public void startClient(String clustername, boolean client);
  public void stopClient();
  public void add(final String indexName, final String type, final long nodeId, final Map<String, String> propertiesValue, boolean upsert);
  public void delete(final String indexName, final String type, final long nodeId);
  public void update(final String indexName, final String type, final long nodeId, final Map<String, String> propertiesValue);  
  public long[] lookup(final long indexId, final String indexName, final Object propertyValue);
}
