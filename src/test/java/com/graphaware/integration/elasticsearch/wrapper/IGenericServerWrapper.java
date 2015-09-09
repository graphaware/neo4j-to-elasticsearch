
package com.graphaware.integration.elasticsearch.wrapper;

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
public interface IGenericServerWrapper
{
  public void startEmbdeddedServer();
  public void stopEmbdeddedServer();
  public void createIndex(String index, Map<String, Object> properties);
}
