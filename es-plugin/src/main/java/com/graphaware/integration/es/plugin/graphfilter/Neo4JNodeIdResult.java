/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.graphaware.integration.es.plugin.graphfilter;

/**
 *
 * @author ale
 */
public class Neo4JNodeIdResult
{
  private long nodeId;
  
  public Neo4JNodeIdResult()
  {
  }

  public long getId()
  {
    return nodeId;
  }
  public void setId(long nodeId)
  {
    this.nodeId = nodeId;
  }  
}
