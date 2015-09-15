
package com.graphaware.integration.es.plugin.filter;

import com.graphaware.integration.es.plugin.query.GAQueryResultNeo4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

public class GAQueryResultNeo4jFilter implements ActionFilter
{
  private static final String SEARCH_REQUEST_INVOKED = "filter.graphaware.neo4j.Invoked";

  protected final ESLogger logger;

  private int order;

  private GAQueryResultNeo4j neo4jConnection;

  @Inject
  public GAQueryResultNeo4jFilter(final Settings settings,
                                  final GAQueryResultNeo4j queryResultCache)
  {
    this.neo4jConnection = queryResultCache;
    logger = Loggers.getLogger(GAQueryResultNeo4jFilter.class.getName(), settings);
  }

  @Override
  public int order()
  {
    return order;
  }

  @Override
  public void apply(final String action,
                    @SuppressWarnings("rawtypes") final ActionRequest request,
                    @SuppressWarnings("rawtypes") final ActionListener listener,
                    final ActionFilterChain chain)
  {
    if (!SearchAction.INSTANCE.name().equals(action))
    {
      chain.proceed(action, request, listener);
      return;
    }

    final SearchRequest searchRequest = (SearchRequest) request;
    final Boolean invoked = searchRequest.getHeader(SEARCH_REQUEST_INVOKED);
    if (invoked != null && invoked.booleanValue())
    {
      @SuppressWarnings("unchecked")
      final ActionListener<SearchResponse> wrappedListener = neo4jConnection
              .wrapActionListener(action, searchRequest, listener);
      chain.proceed(action, request,
              wrappedListener == null ? listener : wrappedListener);
    }
    else
    {
      searchRequest.putHeader(SEARCH_REQUEST_INVOKED, Boolean.TRUE);
      chain.proceed(action, request, listener);
    }

  }

  @Override
  public void apply(final String action, final ActionResponse response,
                    @SuppressWarnings("rawtypes") final ActionListener listener, final ActionFilterChain chain)
  {
    chain.proceed(action, response, listener);
  }

}
