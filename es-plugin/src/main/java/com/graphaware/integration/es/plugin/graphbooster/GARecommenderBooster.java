/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.graphaware.integration.es.plugin.graphbooster;

import com.graphaware.integration.es.plugin.annotation.GAGraphBooster;
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
@GAGraphBooster(name = "GARecommenderBooster")
public class GARecommenderBooster implements IGAResultBooster
{
  private static final Logger logger = Logger.getLogger(GARecommenderBooster.class.getName());
  public static final String INDEX_GA_ES_NEO4J_HOST = "index.ga-es-neo4j.host";
  private String neo4jHost = "http://localhost:7575";

  private int size;
  private int from;
  private String targetId;
  private int maxResultSize = -1;

  public GARecommenderBooster(Settings settings)
  {
    this.neo4jHost = settings.get(INDEX_GA_ES_NEO4J_HOST, neo4jHost);

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
    Map<String, InternalSearchHit> hitIds = new HashMap<>();
    for (InternalSearchHit hit : searchHits)
      hitIds.put(hit.getId(), hit);
    Collection<String> orderedList = externalDoReorder(hitIds.keySet());

    InternalSearchHit[] newSearchHits = new InternalSearchHit[size < orderedList.size() ? size : orderedList.size()];
    logger.log(Level.WARNING, "searchHits.length <= reorderSize: {0}", (searchHits.length <= size));
    int k = 0;
    for (String newId : orderedList)
    {
      newSearchHits[k++] = hitIds.get(newId);
      if (k >= newSearchHits.length)
        break;
    }
    return new InternalSearchHits(newSearchHits, newSearchHits.length,
            hits.maxScore());
  }

  //@Override
  private List<String> externalDoReorder(Collection<String> hitIds)
  {
    logger.log(Level.WARNING, "Query cypher for: {0}", hitIds);

    String recommendationEndopint = neo4jHost
            + "/graphaware/recommendation/filter/"
            //+ "Durgan%20LLC"
            + targetId
            + "?limit=" + Integer.MAX_VALUE
            + "&from=" + from
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
    List<Neo4JResult> results = response.getEntity(type);

    response.close();
    List<String> newSet = new ArrayList<>();
    for (Neo4JResult res : results)
      newSet.add(String.valueOf(res.getUuid() != null ? res.getUuid() : res.getNodeId()));

    return newSet;
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
