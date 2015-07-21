/*
 * Copyright (c) 2014 GraphAware
 *
 * This file is part of GraphAware.
 *
 * GraphAware is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.es;

import com.graphaware.runtime.module.RuntimeModule;
import com.graphaware.runtime.module.RuntimeModuleBootstrapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Bootstraps the {@link EsModule} in server mode.
 */
public class EsBootstrapper implements RuntimeModuleBootstrapper
{

  private static final Logger LOG = LoggerFactory.getLogger(EsBootstrapper.class);

  //keys to use when configuring using neo4j.properties
  private static final String ES_CLASSPATH = "classpath";
  private static final String ES_INDEXNAME = "indexName";

  /**
   * @{inheritDoc}
   */
  @Override
  public RuntimeModule bootstrapModule(String moduleId, Map<String, String> config, GraphDatabaseService database)
  {
    EsConfiguration configuration = EsConfiguration.defaultConfiguration();

    if (config.get(ES_CLASSPATH) != null && config.get(ES_CLASSPATH).length() > 0)
    {
      configuration = configuration.withClasspathDirectoryProperty(config.get(ES_CLASSPATH));
      LOG.info("classpath set to {}", configuration.getClasspathDirectory());
    }

    if (config.get(ES_INDEXNAME) != null && config.get(ES_INDEXNAME).length() > 0)
    {
      configuration = configuration.withIndexName(ES_INDEXNAME);
      LOG.info("indexName set to {}", configuration.getIndexName());
    }
    return new EsModule(moduleId, configuration, database);
  }
}
