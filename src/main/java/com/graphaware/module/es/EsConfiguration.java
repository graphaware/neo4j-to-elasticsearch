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

import com.graphaware.common.policy.InclusionPolicies;
import com.graphaware.runtime.config.BaseTxDrivenModuleConfiguration;
import com.graphaware.runtime.policy.InclusionPoliciesFactory;

/**
 * {@link BaseTxDrivenModuleConfiguration} for
 * {@link com.graphaware.module.es.EsModule}.
 */
public class EsConfiguration extends BaseTxDrivenModuleConfiguration<EsConfiguration>
{

  private static final String DEFAULT_CLASSPATH_DIRECTORY = "esplugin";
  private static final String DEFAULT_INDEX_NAME = "neo4j";
  private static final String DEFAULT_CLUSTERNAME = "neo4j-elasticsearch";

  private String classpathDirectory;
  private String indexName;
  private String clusterName;

  protected EsConfiguration(InclusionPolicies inclusionPolicies)
  {
    super(inclusionPolicies);
  }

  public EsConfiguration(InclusionPolicies inclusionPolicies, String classpathDirectory, String indexName, String clusterName)
  {
    super(inclusionPolicies);
    this.classpathDirectory = classpathDirectory;
    this.indexName = indexName;
    this.clusterName = clusterName;
  }

  /**
   * Create a default configuration with default uuid property =
   * {@link #DEFAULT_UUID_PROPERTY}, uuid index =
   * {@link #DEFAULT_UUID_NODEX_INDEX} labels=all (including nodes with no
   * labels) inclusion strategies =
   * {@link com.graphaware.runtime.policy.InclusionPoliciesFactory#allBusiness()},
   * (nothing is excluded except for framework-internal nodes and relationships)
   * <p/>
   * Change this by calling {@link #withUuidProperty(String)}, with* other
   * inclusion strategies on the object, always using the returned object (this
   * is a fluent interface).
   */
  public static EsConfiguration defaultConfiguration()
  {
    return new EsConfiguration(InclusionPoliciesFactory.allBusiness(), DEFAULT_CLASSPATH_DIRECTORY, DEFAULT_INDEX_NAME, DEFAULT_CLUSTERNAME);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected EsConfiguration newInstance(InclusionPolicies inclusionPolicies)
  {
    return new EsConfiguration(inclusionPolicies, getClasspathDirectory(), getIndexName(), getClusterName());
  }
  public String getClasspathDirectory()
  {
    return classpathDirectory;
  }

  public String getIndexName()
  {
    return indexName;
  }
  
  public String getClusterName()
  {
    return clusterName;
  }
  
  
  /**
   * Create a new instance of this {@link UuidConfiguration} with different uuid
   * property.
   *
   * @param uuidProperty of the new instance.
   * @return new instance.
   */
  
  public EsConfiguration withClasspathDirectoryProperty(String classpathDirectory)
  {
    return new EsConfiguration(getInclusionPolicies(), classpathDirectory, getIndexName(), getClusterName());
  }

  /**
   * Create a new instance of this {@link UuidConfiguration} with different uuid
   * index.
   *
   * @param uuidIndex of the new instance.
   * @return new instance.
   */
  public EsConfiguration withIndexName(String indexName)
  {
    return new EsConfiguration(getInclusionPolicies(), getClasspathDirectory(), indexName, getClusterName());
  }
  
  public EsConfiguration withClusterName(String clusterName)
  {
    return new EsConfiguration(getInclusionPolicies(), getClasspathDirectory(), getIndexName(), clusterName);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object o)
  {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;

    EsConfiguration that = (EsConfiguration) o;

    if (!classpathDirectory.equals(that.classpathDirectory))
      return false;
    if (!indexName.equals(that.indexName))
      return false;

    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + classpathDirectory.hashCode();
    result = 31 * result + indexName.hashCode();
    return result;
  }
}
