
package com.graphaware.integration.es.plugin.graphfilter;

import java.util.Map;
import org.elasticsearch.search.internal.InternalSearchHits;

/**
 *
 * @author ale
 */
public interface IGAResultFilter
{
  public InternalSearchHits doFilter(final InternalSearchHits hits);
  public void parseRequest(Map<String, Object> sourceAsMap);
}
