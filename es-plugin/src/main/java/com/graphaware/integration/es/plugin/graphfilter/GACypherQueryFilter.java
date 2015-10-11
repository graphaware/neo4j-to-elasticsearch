/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.graphaware.integration.es.plugin.graphfilter;

import com.graphaware.integration.es.plugin.annotation.GAGraphFilter;
import com.graphaware.integration.util.GAESUtil;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.core.MediaType;

import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;

/**
 *
 * @author ale
 */
@GAGraphFilter(name = "GACypherQueryFilter")
public class GACypherQueryFilter implements IGAResultFilter
{
  private static final Logger logger = Logger.getLogger(GACypherQueryFilter.class.getName());
  public static final String INDEX_GA_ES_NEO4J_HOST = "index.ga-es-neo4j.host";
  private String neo4jHost = "http://localhost:7575";

  private int size;
  private int from;
  private String cypher;

  public GACypherQueryFilter(Settings settings)
  {
    this.neo4jHost = settings.get(INDEX_GA_ES_NEO4J_HOST, neo4jHost);

  }

  public void parseRequest(Map<String, Object> sourceAsMap)
  {
    size = GAESUtil.getInt(sourceAsMap.get("size"), 10);
    from = GAESUtil.getInt(sourceAsMap.get("from"), 0);

    HashMap extParams = (HashMap) sourceAsMap.get("ga-filter");
    if (extParams != null)
      cypher = (String) extParams.get("query");
  }

  public InternalSearchHits doFilter(final InternalSearchHits hits)
  {
    Set<String> remoteFilter = executeCypher(neo4jHost, cypher);
    final InternalSearchHit[] searchHits = hits.internalHits();
    Map<String, InternalSearchHit> hitMap = new HashMap<>();
    for (InternalSearchHit hit : searchHits)
      hitMap.put(hit.getId(), hit);

    InternalSearchHit[] tmpSearchHits = new InternalSearchHit[hitMap.size()];
    int k = 0;
    for (Map.Entry<String, InternalSearchHit> item : hitMap.entrySet())
    {
      if (remoteFilter.contains(item.getKey()))
      {
        tmpSearchHits[k] = item.getValue();
        k++;
      }
    }
    
    logger.log(Level.WARNING, "searchHits.length <= reorderSize: {0}", (searchHits.length <= size));
    InternalSearchHit[] newSearchHits = new InternalSearchHit[k];
    k = 0;
    for (InternalSearchHit newId : tmpSearchHits)
    {
      if (newId == null)
        break;
      newSearchHits[k++] = newId;
    }
    return new InternalSearchHits(newSearchHits, newSearchHits.length,
            hits.maxScore());
  }

  
  public int getSize()
  {
    return size;
  }

  public int getFrom()
  {
    return from;
  }

  public Set<String> executeCypher(String serverUrl, String... cypherStatements)
  {
    StringBuilder stringBuilder = new StringBuilder("{\"statements\" : [");
    for (String statement : cypherStatements)
    {
      stringBuilder.append("{\"statement\" : \"").append(statement).append("\"}").append(",");
    }
    stringBuilder.deleteCharAt(stringBuilder.length() - 1);

    stringBuilder.append("]}");

    while (serverUrl.endsWith("/"))
    {
      serverUrl = serverUrl.substring(0, serverUrl.length() - 1);
    }

    return post(serverUrl + "/db/data/transaction/commit", stringBuilder.toString());
  }

  public Set<String> post(String url, String json)
  {
    ClientConfig cfg = new DefaultClientConfig();
    cfg.getClasses().add(JacksonJsonProvider.class);
    WebResource resource = Client.create(cfg).resource(url);
    ClientResponse response = resource
            .accept( MediaType.APPLICATION_JSON )
            .type( MediaType.APPLICATION_JSON )
            .entity( json )
            .post(ClientResponse.class);
    GenericType<Map<String, Object>> type = new GenericType<Map<String, Object>>(){};
    Map<String, Object> results = response.getEntity(type);

//    System.out.println(String.format("\n\n\n\n\n\n\nGET to [%s], status code [%d], returned data: "
//            + System.getProperty("line.separator") + "%s \n\n\n\n\n\n",
//            url, response.getStatus(), results));
    Map res = (Map) ((ArrayList)results.get("results")).get(0);
    ArrayList<LinkedHashMap> rows = (ArrayList)res.get("data");
    response.close();
    Set<String> newSet = new HashSet<>();
    for (Iterator<LinkedHashMap> it = rows.iterator(); it.hasNext();)
    {
      String nodeId = (String) ((ArrayList)(it.next().get("row"))).get(0);
      newSet.add(String.valueOf(nodeId));
    }
    return newSet;
  }

}
