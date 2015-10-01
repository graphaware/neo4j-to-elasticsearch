/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.graphaware.elasticsearch.reco.demo;

import io.searchbox.annotations.JestId;

/**
 *
 * @author ale
 */
public class JestPersonResult
{

  @JestId
  private String documentId;
  
  private String name;
  
  public String getDocumentId()
  {
    return documentId;
  }
  public String getName()
  {
    return name;
  }
  public void setName(String name)
  {
    this.name = name;
  }
}
