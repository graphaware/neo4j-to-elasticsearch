/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.graphaware.integration.elasticsearch.plugin.query;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.ws.rs.core.MediaType;
import org.elasticsearch.common.settings.Settings;

/**
 *
 * @author ale
 */
public class Neo4JRecommenderBooster implements QueryResultBooster
{
  public static final String INDEX_GA_ES_NEO4J_HOST = "index.ga-es-neo4j.host";

  private String neo4jHost = "http://localhost:7575";

  public Neo4JRecommenderBooster(Settings settings)
  {
    this.neo4jHost = settings.get(INDEX_GA_ES_NEO4J_HOST, neo4jHost);

  }
  @Override
  public List<String> doReorder(String nodeId, Collection<String> hitIds, int reorderSize)
  {
    String recommendationEndopint = neo4jHost 
            + "/graphaware/recommendation/filter/" 
            + "Durgan%20LLC"
            //+ nodeId 
            + "?ids=";
    boolean isFirst = true;
    for (String id : hitIds)
    {
      if (!isFirst)
        recommendationEndopint = recommendationEndopint.concat(",");
      isFirst = false;
      recommendationEndopint = recommendationEndopint.concat(id);
    }
    WebResource resource = Client.create().resource( recommendationEndopint );
    ClientResponse response = resource
            .accept(MediaType.APPLICATION_JSON)
            .get(ClientResponse.class);
    final String entity = response.getEntity(String.class);

    System.out.println(String.format("\n\n\n\n\n\n\nGET to [%s], status code [%d], returned data: "
            + System.getProperty("line.separator") + "%s \n\n\n\n\n\n",
            recommendationEndopint, response.getStatus(), entity));

    response.close();
    
    List<String> newSet = new ArrayList<>();
    int i = 0;
    for (String key : hitIds)
    {
      if (i < reorderSize)
        newSet.add(key);
      else
        break;
      i++;
    }
    return newSet;
  }

}
