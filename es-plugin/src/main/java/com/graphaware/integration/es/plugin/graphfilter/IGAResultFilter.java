
package com.graphaware.integration.es.plugin.graphfilter;

import java.util.Collection;
import java.util.List;

/**
 *
 * @author ale
 */
public interface IGAResultFilter
{
  public List<String> doFilter(Collection<String> hitIds);
}
