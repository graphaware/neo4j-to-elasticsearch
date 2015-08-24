
package com.graphaware.integration.elasticsearch.plugin.query;


import java.io.IOException;
import java.util.Map;
import com.graphaware.integration.elasticsearch.plugin.GAQueryResultNeo4jPlugin;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import static org.elasticsearch.action.search.ShardSearchFailure.readShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.facet.InternalFacets;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import static org.elasticsearch.search.internal.InternalSearchHits.readSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.threadpool.ThreadPool;

public class GAQueryResultNeo4j extends AbstractComponent
{

  public static final String INDEX_GA_ES_NEO4J_ENABLED = "index.ga-es-neo4j.enable";
  public static final String INDEX_GA_ES_NEO4J_HOST = "index.ga-es-neo4j.host";
  private static final String DYNARANK_RERANK_ENABLE = "_rerank";

  protected final ESLogger logger;

  private final ThreadPool threadPool;

  private ClusterService clusterService;

  private boolean enabled;

  private Client client;
  
  private String neo4jHost = "http://localhost:7474";

  @Inject
  public GAQueryResultNeo4j(final Settings settings,
                            final ClusterService clusterService, 
                            final ThreadPool threadPool)
  {
    super(settings);
    this.clusterService = clusterService;
    this.threadPool = threadPool;
    this.logger = Loggers.getLogger(GAQueryResultNeo4jPlugin.INDEX_LOGGER_NAME, settings);
    this.enabled = settings.getAsBoolean(INDEX_GA_ES_NEO4J_ENABLED, false);
    this.neo4jHost = settings.get(INDEX_GA_ES_NEO4J_HOST, "http://localhost:7474");
  }

  public SearchResponse process(SearchResponse response)
  {
    final long startTime = System.nanoTime();
    try
    {
      final BytesStreamOutput out = new BytesStreamOutput();
      response.writeTo(out);

      if (logger.isDebugEnabled())
      {
        logger.debug("Reading headers...");
      }
      final BytesStreamInput in = new BytesStreamInput(
              out.bytes());
      Map<String, Object> headers = null;
      if (in.readBoolean())
      {
        headers = in.readMap();
      }
      if (logger.isDebugEnabled())
      {
        logger.debug("Reading hits...");
      }
      final InternalSearchHits hits = readSearchHits(in);
      InternalSearchHits newHits = doReorder(hits, 5, "1");
      InternalFacets facets = null;
      if (in.readBoolean())
      {
        facets = InternalFacets.readFacets(in);
      }
      if (logger.isDebugEnabled())
      {
        logger.debug("Reading aggregations...");
      }
      InternalAggregations aggregations = null;
      if (in.readBoolean())
      {
        aggregations = InternalAggregations
                .readAggregations(in);
      }
      if (logger.isDebugEnabled())
      {
        logger.debug("Reading suggest...");
      }
      Suggest suggest = null;
      if (in.readBoolean())
      {
        suggest = Suggest.readSuggest(Suggest.Fields.SUGGEST,
                in);
      }
      final boolean timedOut = in.readBoolean();
      Boolean terminatedEarly = null;
      if (in.getVersion().onOrAfter(Version.V_1_4_0_Beta1))
      {
        terminatedEarly = in.readOptionalBoolean();
      }
      final InternalSearchResponse internalResponse = new InternalSearchResponse(
              newHits, facets, aggregations, suggest, timedOut,
              terminatedEarly);
      final int totalShards = in.readVInt();
      final int successfulShards = in.readVInt();
      final int size = in.readVInt();
      ShardSearchFailure[] shardFailures;
      if (size == 0)
      {
        shardFailures = ShardSearchFailure.EMPTY_ARRAY;
      }
      else
      {
        shardFailures = new ShardSearchFailure[size];
        for (int i = 0; i < shardFailures.length; i++)
        {
          shardFailures[i] = readShardSearchFailure(in);
        }
      }
      final String scrollId = in.readOptionalString();
      final long tookInMillis = response.getTookInMillis() + (System.nanoTime() - startTime) / 1000000;

      if (logger.isDebugEnabled())
      {
        logger.debug("Creating new SearchResponse...");
      }
      final SearchResponse newResponse = new SearchResponse(
              internalResponse, scrollId, totalShards,
              successfulShards, tookInMillis, shardFailures);
      if (headers != null)
      {
        for (final Map.Entry<String, Object> entry : headers
                .entrySet())
        {
          newResponse.putHeader(entry.getKey(),
                  entry.getValue());
        }
      }
      if (logger.isDebugEnabled())
      {
        logger.debug("Rewriting overhead time: {} - {} = {}ms",
                tookInMillis, response.getTookInMillis(),
                tookInMillis - response.getTookInMillis());
      }
      return newResponse;
    }
    catch (IOException ex)
    {
      return null;
    }

  }
  private InternalSearchHits doReorder(final InternalSearchHits hits, int reorderSize, String userId)
  {
    final InternalSearchHit[] searchHits = hits.internalHits();
    Map<String, InternalSearchHit> hitIds = new HashMap<>();
    for (InternalSearchHit hit : searchHits)
      hitIds.put(hit.getId(), hit);
    Collection<String> orderedList = externalDoReorder(hitIds.keySet(), reorderSize);
    
    InternalSearchHit[] newSearchHits = new InternalSearchHit[reorderSize < searchHits.length ? reorderSize : searchHits.length];
    if (logger.isDebugEnabled())
    {
      logger.debug("searchHits.length <= reorderSize: {}",
              searchHits.length <= reorderSize);
    }
    int k = 0;
    for (String newId : orderedList)
      newSearchHits[k++] = hitIds.get(newId);
    return new InternalSearchHits(newSearchHits, newSearchHits.length,
            hits.maxScore());
  }

  public ActionListener<SearchResponse> wrapActionListener(
          final String action, final SearchRequest request,
          final ActionListener<SearchResponse> listener)
  {
    switch (request.searchType())
    {
      case DFS_QUERY_AND_FETCH:
      case QUERY_AND_FETCH:
      case QUERY_THEN_FETCH:
        break;
      default:
        return null;
    }

    if (request.scroll() != null)
    {
      return null;
    }

    final Object isRerank = request.getHeader(DYNARANK_RERANK_ENABLE);
    if (isRerank instanceof Boolean && !((Boolean) isRerank).booleanValue())
    {
      return null;
    }

    BytesReference source = request.source();
    if (source == null)
    {
      source = request.extraSource();
      if (source == null)
        return null;
    }

    final String[] indices = request.indices();
    if (indices == null || indices.length != 1)
    {
      return null;
    }

//        final String index = indices[0];
//        final ScriptInfo scriptInfo = getScriptInfo(index);
//        if (scriptInfo == null || scriptInfo.getScript() == null) {
//            return null;
//        }
    final long startTime = System.nanoTime();

    try
    {
      final Map<String, Object> sourceAsMap = SourceLookup
              .sourceAsMap(source);
      final int size = getInt(sourceAsMap.get("size"), 10);
      final int from = getInt(sourceAsMap.get("from"), 0);
      if (size < 0 || from < 0)
      {
        return null;
      }

//            if (from >= scriptInfo.getReorderSize()) {
//                return null;
//            }
//
//            int maxSize = scriptInfo.getReorderSize();
//            if (from + size > scriptInfo.getReorderSize()) {
//                maxSize = from + size;
//            }
      int maxSize = 5;
      sourceAsMap.put("size", maxSize);
      sourceAsMap.put("from", 0);

      if (logger.isDebugEnabled())
      {
        logger.debug("Rewrite query: from:{}->{} size:{}->{}", from, 0,
                size, maxSize);
      }

      final XContentBuilder builder = XContentFactory
              .contentBuilder(Requests.CONTENT_TYPE);
      String userId = findUser(sourceAsMap);
      builder.map(sourceAsMap);
      request.source(builder.bytes());
      
      //String size = findSize(sourceAsMap);

      final ActionListener<SearchResponse> searchResponseListener
              = createSearchResponseListener(listener, from, size, 5, startTime, userId);
      return new ActionListener<SearchResponse>()
      {
        @Override
        public void onResponse(SearchResponse response)
        {
          try
          {
            searchResponseListener.onResponse(response);
          }
          catch (RetrySearchException e)
          {
            Map<String, Object> newSourceAsMap = e.rewrite(sourceAsMap);
            if (newSourceAsMap == null)
            {
              throw new RuntimeException("Failed to rewrite source: " + sourceAsMap);
            }
            newSourceAsMap.put("size", size);
            newSourceAsMap.put("from", from);
            if (logger.isDebugEnabled())
            {
              logger.debug("Original Query: \n{}\nNew Query: \n{}", sourceAsMap, newSourceAsMap);
            }
            try
            {
              final XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE);
              builder.map(newSourceAsMap);
              request.source(builder.bytes());
              for (String name : request.getHeaders())
              {
                if (name.startsWith("filter.codelibs."))
                {
                  request.putHeader(name, Boolean.FALSE);
                }
              }
              request.putHeader(DYNARANK_RERANK_ENABLE, Boolean.FALSE);
              client.search(request, listener);
            }
            catch (IOException ioe)
            {
              throw new RuntimeException("Failed to parse a new source.", ioe);
            }
          }
        }

        @Override
        public void onFailure(Throwable t)
        {
          searchResponseListener.onFailure(t);
        }
      };
    }
    catch (final IOException e)
    {
      throw new RuntimeException("Failed to parse a source.", e);
    }
  }

  private ActionListener<SearchResponse> createSearchResponseListener(
          final ActionListener<SearchResponse> listener, final int from,
          final int size, final int reorderSize, final long startTime, final String userId)
  {
    return new ActionListener<SearchResponse>()
    {
      @Override
      public void onResponse(final SearchResponse response)
      {
        if (response.getHits().getTotalHits() == 0)
        {
          if (logger.isDebugEnabled())
          {
            logger.debug("No reranking results: {}", response);
          }
          listener.onResponse(response);
          return;
        }

        if (logger.isDebugEnabled())
        {
          logger.debug("Reranking results: {}", response);
        }

        try
        {
          final BytesStreamOutput out = new BytesStreamOutput();
          response.writeTo(out);

          if (logger.isDebugEnabled())
          {
            logger.debug("Reading headers...");
          }
          final BytesStreamInput in = new BytesStreamInput(
                  out.bytes());
          Map<String, Object> headers = null;
          if (in.readBoolean())
          {
            headers = in.readMap();
          }
          if (logger.isDebugEnabled())
          {
            logger.debug("Reading hits...");
          }
          final InternalSearchHits hits = readSearchHits(in);
          final InternalSearchHits newHits = doReorder(hits, reorderSize, userId);
          InternalFacets facets = null;
          if (in.readBoolean())
          {
            facets = InternalFacets.readFacets(in);
          }
          if (logger.isDebugEnabled())
          {
            logger.debug("Reading aggregations...");
          }
          InternalAggregations aggregations = null;
          if (in.readBoolean())
          {
            aggregations = InternalAggregations
                    .readAggregations(in);
          }
          if (logger.isDebugEnabled())
          {
            logger.debug("Reading suggest...");
          }
          Suggest suggest = null;
          if (in.readBoolean())
          {
            suggest = Suggest.readSuggest(Suggest.Fields.SUGGEST,
                    in);
          }
          final boolean timedOut = in.readBoolean();
          Boolean terminatedEarly = null;
          if (in.getVersion().onOrAfter(Version.V_1_4_0_Beta1))
          {
            terminatedEarly = in.readOptionalBoolean();
          }
          final InternalSearchResponse internalResponse = new InternalSearchResponse(
                  newHits, facets, aggregations, suggest, timedOut,
                  terminatedEarly);
          final int totalShards = in.readVInt();
          final int successfulShards = in.readVInt();
          final int size = in.readVInt();
          ShardSearchFailure[] shardFailures;
          if (size == 0)
          {
            shardFailures = ShardSearchFailure.EMPTY_ARRAY;
          }
          else
          {
            shardFailures = new ShardSearchFailure[size];
            for (int i = 0; i < shardFailures.length; i++)
            {
              shardFailures[i] = readShardSearchFailure(in);
            }
          }
          final String scrollId = in.readOptionalString();
          final long tookInMillis = (System.nanoTime() - startTime) / 1000000;

          if (logger.isDebugEnabled())
          {
            logger.debug("Creating new SearchResponse...");
          }
          final SearchResponse newResponse = new SearchResponse(
                  internalResponse, scrollId, totalShards,
                  successfulShards, tookInMillis, shardFailures);
          if (headers != null)
          {
            for (final Map.Entry<String, Object> entry : headers
                    .entrySet())
            {
              newResponse.putHeader(entry.getKey(),
                      entry.getValue());
            }
          }
          listener.onResponse(newResponse);

          if (logger.isDebugEnabled())
          {
            logger.debug("Rewriting overhead time: {} - {} = {}ms",
                    tookInMillis, response.getTookInMillis(),
                    tookInMillis - response.getTookInMillis());
          }

        }
        catch (final RetrySearchException e)
        {
          throw e;
        }
        catch (final Exception e)
        {
          if (logger.isDebugEnabled())
          {
            logger.debug("Failed to parse a search response.", e);
          }
          throw new RuntimeException(
                  "Failed to parse a search response.", e);
        }
      }

      @Override
      public void onFailure(final Throwable e)
      {
        listener.onFailure(e);
      }
    };
  }
  private int getInt(final Object value, final int defaultValue)
  {
    if (value instanceof Number)
    {
      return ((Number) value).intValue();
    }
    else if (value instanceof String)
    {
      return Integer.parseInt(value.toString());
    }
    return defaultValue;
  }
  private Collection<String> externalDoReorder(Collection<String> hitIds, int reorderSize)
  {
    logger.warn("Query cypher for: " + hitIds);
    Set<String> newSet = new HashSet<>();
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
  private String findUser(Object sourceAsMap)
  {
    if (!(sourceAsMap instanceof Map))
      return null;
    Map<String, Object> current = (Map<String, Object>)sourceAsMap;
    final Object __forUser = current.get("__forUser");
    if (__forUser != null)
    {
      final String userId = (String)((Map<String, Object>)((Map<String, Object>)current).get("__forUser")).get("query");
      //current.remove("__forUser");
      return userId;
    }
    for (Object child : current.values())
    {
      String result = findUser(child);
      if (result != null)
        return result;
    }
    return null;
  }
}
