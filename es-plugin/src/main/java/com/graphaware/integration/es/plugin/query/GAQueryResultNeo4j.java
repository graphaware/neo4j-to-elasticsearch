
package com.graphaware.integration.es.plugin.query;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.graphaware.integration.es.plugin.GAQueryResultNeo4jPlugin;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
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
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.facet.InternalFacets;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.search.ShardSearchFailure.readShardSearchFailure;
import static org.elasticsearch.search.internal.InternalSearchHits.readSearchHits;

public class GAQueryResultNeo4j extends AbstractComponent
{

  public static final String INDEX_GA_ES_NEO4J_ENABLED = "index.ga-es-neo4j.enable";
  public static final String INDEX_GA_ES_NEO4J_REORDER_TYPE = "index.dynarank.reorder_size";

  private static final String DYNARANK_RERANK_ENABLE = "_rerank";

  protected final ESLogger logger;

  private final ThreadPool threadPool;

  private ClusterService clusterService;

  private boolean enabled;

  private Client client;
  private Cache<String, ScriptInfo> scriptInfoCache;

  private QueryResultBooster booster;

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
    this.booster = new Neo4JRecommenderBooster(settings);
    final CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder()
            .concurrencyLevel(16);
    builder.expireAfterAccess(120, TimeUnit.SECONDS);
    scriptInfoCache = builder.build();

  }

  private InternalSearchHits doReorder(final InternalSearchHits hits, int reorderSize, String userId)
  {
    final InternalSearchHit[] searchHits = hits.internalHits();
    Map<String, InternalSearchHit> hitIds = new HashMap<>();
    for (InternalSearchHit hit : searchHits)
      hitIds.put(hit.getId(), hit);
    Collection<String> orderedList = externalDoReorder(hitIds.keySet(), reorderSize, userId);

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

    //TODO: from here we should get some infos like max size in response, 
    //the name of the rescorer (class name for example).
    //These configuration should be defined at index level and cached
    final String index = indices[0];
    final ScriptInfo scriptInfo = getScriptInfo(index);
    
    if (scriptInfo != null)
    {
      logger.warn(">>>>>>>>>>>> Type: " + scriptInfo.getClassname());
    }
    final long startTime = System.nanoTime();

    try
    {
      final Map<String, Object> sourceAsMap = SourceLookup
              .sourceAsMap(source);
      final int size = getInt(sourceAsMap.get("size"), 10);
      final int from = getInt(sourceAsMap.get("from"), 0);

      HashMap extParams = (HashMap) sourceAsMap.get("ga-params");
      int reorderSize = 10;
      String userId = "";
      if (extParams != null)
      {
        userId = (String) extParams.get("recoTarget");
        reorderSize = getInt(extParams.get("reorderSize"), 0);
        sourceAsMap.remove("ga-params");
      }

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
      int maxSize = Integer.MAX_VALUE; //Get complete response to can reorder (may be some parameter could reduce the dimension)
      sourceAsMap.put("size", maxSize);
      sourceAsMap.put("from", 0);

      if (logger.isDebugEnabled())
      {
        logger.debug("Rewrite query: from:{}->{} size:{}->{}", from, 0,
                size, maxSize);
      }

      final XContentBuilder builder = XContentFactory
              .contentBuilder(Requests.CONTENT_TYPE);
      //String userId = findUser(sourceAsMap);
      builder.map(sourceAsMap);
      request.source(builder.bytes());

      //String size = findSize(sourceAsMap);
      final ActionListener<SearchResponse> searchResponseListener
              = createSearchResponseListener(listener, from, size, reorderSize, startTime, userId);
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
          final SearchResponse newResponse = handleResponse(response, startTime, reorderSize, userId);
          listener.onResponse(newResponse);
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

  private SearchResponse handleResponse(final SearchResponse response, final long startTime, final int reorderSize, final String userId) throws IOException
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

    if (logger.isDebugEnabled())
    {
      logger.debug("Rewriting overhead time: {} - {} = {}ms",
              tookInMillis, response.getTookInMillis(),
              tookInMillis - response.getTookInMillis());
    }
    return newResponse;
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

  private Collection<String> externalDoReorder(Collection<String> hitIds, int reorderSize, String userId)
  {
    logger.warn("Query cypher for: " + hitIds);

    List<String> newOrderedHotsId = booster.doReorder(userId, hitIds, reorderSize);
    return newOrderedHotsId;
  }

  public ScriptInfo getScriptInfo(final String index)
  {
    try
    {
      return scriptInfoCache.get(index, new Callable<ScriptInfo>()
      {
        @Override
        public ScriptInfo call() throws Exception
        {
          final MetaData metaData = clusterService.state()
                  .getMetaData();
          String[] concreteIndices = metaData.concreteIndices(
                  IndicesOptions.strictExpandOpenAndForbidClosed(),
                  index);
          Settings indexSettings = null;
          for (String concreteIndex : concreteIndices)
          {
            IndexMetaData indexMD = metaData.index(concreteIndex);
            if (indexMD != null)
            {
              final Settings scriptSettings = indexMD.settings();
              final String script = scriptSettings.get(INDEX_GA_ES_NEO4J_REORDER_TYPE);
              if (script != null && script.length() > 0)
              {
                indexSettings = scriptSettings;
              }
            }
          }

          if (indexSettings == null)
          {
            return ScriptInfo.NO_SCRIPT_INFO;
          }

          return new ScriptInfo(indexSettings.get(INDEX_GA_ES_NEO4J_REORDER_TYPE));
        }
      });
    }
    catch (final Exception e)
    {
      logger.warn("Failed to load ScriptInfo for {}.", e, index);
      return null;
    }
  }

  public static class ScriptInfo
  {
    protected final static ScriptInfo NO_SCRIPT_INFO = new ScriptInfo();

    private String script;

    private String lang;

    private ScriptService.ScriptType scriptType;

    private Map<String, Object> settings;

    private int reorderSize;
    private String classname;

    ScriptInfo()
    {
      // nothing
    }

    ScriptInfo(final String classname)
    {
      this.classname = classname;
    }

    public String getScript()
    {
      return script;
    }

    public String getLang()
    {
      return lang;
    }

    public ScriptService.ScriptType getScriptType()
    {
      return scriptType;
    }

    public Map<String, Object> getSettings()
    {
      return settings;
    }

    public int getReorderSize()
    {
      return reorderSize;
    }

    @Override
    public String toString()
    {
      return "ScriptInfo [script=" + script + ", lang=" + lang
              + ", scriptType=" + scriptType + ", settings=" + settings
              + ", reorderSize=" + reorderSize + "]";
    }
    
    public String getClassname()
    {
      return classname;
    }
    
  }
}
