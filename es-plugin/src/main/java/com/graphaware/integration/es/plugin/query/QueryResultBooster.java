/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.graphaware.integration.es.plugin.query;

import java.util.Collection;
import java.util.List;

/**
 *
 * @author ale
 */
public interface QueryResultBooster
{
  public List<String> doReorder(String nodeId, Collection<String> hitIds, int reorderSize);
  //public 
}
