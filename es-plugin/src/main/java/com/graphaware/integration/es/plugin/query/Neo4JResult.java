/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.graphaware.integration.es.plugin.query;

/**
 *
 * @author ale
 */
public class Neo4JResult
{
  private long nodeId;
  private String uuid;
  private String item;
  public Neo4JResult()
  {
  }

  public long getNodeId()
  {
    return nodeId;
  }
  public void setNodeId(long nodeId)
  {
    this.nodeId = nodeId;
  }
  public String getUuid()
  {
    return uuid;
  }
  public void setUuid(String uuid)
  {
    this.uuid = uuid;
  }
  public String getItem()
  {
    return item;
  }
  public void setItem(String item)
  {
    this.item = item;
  }

}
