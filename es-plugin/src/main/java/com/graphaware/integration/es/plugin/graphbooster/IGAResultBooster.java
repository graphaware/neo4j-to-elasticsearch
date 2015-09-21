
package com.graphaware.integration.es.plugin.graphbooster;

import java.util.Collection;
import java.util.List;

/**
 *
 * @author ale
 */
public interface IGAResultBooster
{
  public List<String> doReorder(String nodeId, Collection<String> hitIds, int reorderSize);

}
