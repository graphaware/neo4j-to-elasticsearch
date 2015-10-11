
package com.graphaware.integration.es.plugin.graphbooster;

import java.util.Map;
import org.elasticsearch.search.internal.InternalSearchHits;

/**
 *
 * @author ale
 */
public interface IGAResultBooster
{
  public InternalSearchHits doReorder(final InternalSearchHits hits);
  public void parseRequest(Map<String, Object> sourceAsMap);
}
