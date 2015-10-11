
package com.graphaware.integration.es.plugin;

import java.util.Collection;

//import org.codelibs.elasticsearch.extension.module.ExtensionModule;
import com.graphaware.integration.es.plugin.filter.GAQueryResultNeo4jFilter;
import com.graphaware.integration.es.plugin.module.GAQueryResultNeo4jModule;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.index.settings.IndexDynamicSettingsModule;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

public class GAQueryResultNeo4jPlugin extends AbstractPlugin
{
  public static final String REST_LOGGER_NAME = "rest.action.admin.ga-es-neo4j";

  public static final String INDEX_LOGGER_NAME = "index.es-neo4j";

  @Override
  public String name()
  {
    return "GAQueryResultNeo4jPlugin";
  }

  @Override
  public String description()
  {
    return "This is Graph Aware plugin for Neo4j.";
  }

  public void onModule(final ActionModule module)
  {
    module.registerFilter(GAQueryResultNeo4jFilter.class);
  }

//  public void onModule(final ExtensionModule module)
//  {
//    //module.registerEngineFilter(RefreshEngineFilter.class);
//  }

  public void onModule(final RestModule module)
  {
//    module.addRestAction(RestClearQRCacheAction.class);
//    module.addRestAction(RestStatsQRCacheAction.class);
  }

  public void onModule(final IndexDynamicSettingsModule module)
  {
    module.addDynamicSettings("index.ga-es-neo4j.enable.*");
  }

  @Override
  public Collection<Class<? extends Module>> modules()
  {
    final Collection<Class<? extends Module>> modules = Lists
            .newArrayList();
    modules.add(GAQueryResultNeo4jModule.class);
    return modules;
  }

}
