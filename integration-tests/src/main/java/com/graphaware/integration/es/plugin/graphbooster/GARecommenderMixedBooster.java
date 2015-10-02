/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.graphaware.integration.es.plugin.graphbooster;

import com.graphaware.integration.es.plugin.annotation.GAGraphBooster;
import com.graphaware.integration.es.plugin.query.GAQueryResultNeo4j;
import com.graphaware.integration.util.GAESUtil;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
@GAGraphBooster(name = "GARecommenderMixedBooster")
public class GARecommenderMixedBooster implements IGAResultBooster
{
  private static final Logger logger = Logger.getLogger(GARecommenderMixedBooster.class.getName());
  public static final String INDEX_GA_ES_NEO4J_HOST = "index.ga-es-neo4j.host";
  private String neo4jHost = "http://localhost:7575";

  private int size;
  private int from;
  private String targetId;
  private int maxResultSize = -1;
  private String keyProperty = GAQueryResultNeo4j.DEFAULT_KEY_PROPERTY;


  public GARecommenderMixedBooster(Settings settings)
  {
    this.neo4jHost = settings.get(INDEX_GA_ES_NEO4J_HOST, neo4jHost);
    this.keyProperty = settings.get(GAQueryResultNeo4j.INDEX_GA_ES_NEO4J_KEY_PROPERTY, GAQueryResultNeo4j.DEFAULT_KEY_PROPERTY);
  }

  public void parseRequest(Map<String, Object> sourceAsMap)
  {
    size = GAESUtil.getInt(sourceAsMap.get("size"), 10);
    from = GAESUtil.getInt(sourceAsMap.get("from"), 0);

    HashMap extParams = (HashMap) sourceAsMap.get("ga-booster");
    if (extParams != null)
    {
      targetId = (String) extParams.get("recoTarget");
      maxResultSize = GAESUtil.getInt(extParams.get("maxResultSize"), Integer.MAX_VALUE);
      keyProperty = (String) (extParams.get("keyProperty") != null ? extParams.get("keyProperty") : keyProperty);
    }
    if (maxResultSize > 0)
    {
      sourceAsMap.put("size", maxResultSize);
      sourceAsMap.put("from", 0);
    }
  }

  public InternalSearchHits doReorder(final InternalSearchHits hits)
  {
    final InternalSearchHit[] searchHits = hits.internalHits();
    Map<String, InternalSearchHit> hitMap = new HashMap<>();
    for (InternalSearchHit hit : searchHits)
      hitMap.put(hit.getId(), hit);
    
    Map<String, Neo4JResult> remoteScore = externalDoReorder(hitMap.keySet());
    final int arraySize = size < searchHits.length ? size : searchHits.length;
    List<InternalSearchHit> newSearchHits = new ArrayList<>(arraySize);
    
    for (Map.Entry<String, InternalSearchHit> item : hitMap.entrySet())
    {
      Neo4JResult remoteResult = remoteScore.get(item.getKey());
      if (remoteResult != null)
        item.getValue().score(item.getValue().score()*remoteResult.getScore());
      int k = 0;
      while (newSearchHits.size() > 0 && k < newSearchHits.size() && newSearchHits.get(k) != null && newSearchHits.get(k).score() > item.getValue().score() && k < arraySize)
        k++;
      if (k < arraySize)
        newSearchHits.add(k,item.getValue());
      if (newSearchHits.size() > arraySize)
        newSearchHits.remove(arraySize);
    }
    return new InternalSearchHits(newSearchHits.toArray(new InternalSearchHit[arraySize]), newSearchHits.size(),
            hits.maxScore());
  }

  //@Override
  private Map<String, Neo4JResult> externalDoReorder(Collection<String> hitIds)
  {
    logger.log(Level.WARNING, "Query cypher for: {0}", hitIds);

    String recommendationEndopint = neo4jHost
            + "/graphaware/recommendation/filter/"
            //+ "Durgan%20LLC"
            + targetId
            + "?limit=" + Integer.MAX_VALUE
            + "&from=" + from
            + "&keyProperty=" + keyProperty
            + "&ids=";
    boolean isFirst = true;
    for (String id : hitIds)
    {
      if (!isFirst)
        recommendationEndopint = recommendationEndopint.concat(",");
      isFirst = false;
      recommendationEndopint = recommendationEndopint.concat(id);
    }
    ClientConfig cfg = new DefaultClientConfig();
    cfg.getClasses().add(JacksonJsonProvider.class);
    WebResource resource = Client.create(cfg).resource(recommendationEndopint);
    ClientResponse response = resource
            .accept(MediaType.APPLICATION_JSON)
            .get(ClientResponse.class);
    GenericType<List<Neo4JResult>> type = new GenericType<List<Neo4JResult>>()
    {
    };
    List<Neo4JResult> res = response.getEntity(type);
    response.close();
    
    HashMap<String, Neo4JResult> results = new HashMap<>();
    
    for (Neo4JResult item : res)
      results.put(String.valueOf(item.getUuid() != null ? item.getUuid() : item.getNodeId()), item);
    
    return results;
  }
  public int getSize()
  {
    return size;
  }

  public int getFrom()
  {
    return from;
  }
  public int getMaxResultSize()
  {
    return maxResultSize;
  }

}
