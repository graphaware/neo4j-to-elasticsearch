/*
 * Copyright (c) 2013-2016 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.es.search.resolver;

import org.neo4j.graphdb.GraphDatabaseService;

public abstract class KeyToIdResolver {

    /**
     * Should throw when the current resolver is not applicable.
     *
     * @param database
     * @param keyProperty
     * @throws Exception
     */
    KeyToIdResolver(final GraphDatabaseService database, final String keyProperty) throws ResolverNotApplicable {

    }

    /**
     * Resolve the Neo4j ID from the ElasticSearch ID.
     *
     * @param key Value if the "_id" field in ElasticSearch, extracted using {@link com.graphaware.module.es.mapping.BaseMapping#getKey}
     * @return the native Neo4j ID of the node
     */
    public abstract long getNodeID(final String key);

    /**
     * Resolve the Neo4j ID from the ElasticSearch ID.
     *
     * @param key Value if the "_id" field in ElasticSearch, extracted using {@link com.graphaware.module.es.mapping.BaseMapping#getKey}
     * @return the native Neo4j ID of the relationship
     */
    public abstract long getRelationshipID(final String key);
}
