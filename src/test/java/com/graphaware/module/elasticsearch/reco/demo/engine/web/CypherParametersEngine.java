/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.graphaware.module.elasticsearch.reco.demo.engine.web;

import com.graphaware.reco.generic.config.MapBasedConfig;
import com.graphaware.reco.generic.context.Context;
import com.graphaware.reco.neo4j.engine.CypherEngine;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.graphdb.Node;

/**
 *
 * @author ale
 */
public class CypherParametersEngine extends CypherEngine
{
  public CypherParametersEngine(String name, String query)
  {
    super(name, query);
  }
  
  protected Map<String, Object> buildParams(Node input, Context<Node, Node> context) {
        Map<String, Object> params = new HashMap<>();
        params.put(idParamName(), input.getId());
        params.put(idsParamName(), input.getId());  
        if (context.config() instanceof MapBasedConfig)
          params.put(limitParamName(), ((MapBasedConfig)context.config()).get("ids"));
        return params;
    }
  private String idsParamName()
  {
    return "ids";
  }
}
