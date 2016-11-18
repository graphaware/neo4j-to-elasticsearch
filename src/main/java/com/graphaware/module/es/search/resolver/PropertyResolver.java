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
import org.neo4j.graphdb.Result;

import java.util.HashMap;
import java.util.Map;

/**
 * Not used yet.
 * Needs to be tested.
 * Sub-optimal performances.
 */
public class PropertyResolver extends KeyToIdResolver {
    private static final String NODE_QUERY = "MATCH (n) WHERE n.`{propertyKey}` = \"{keyValue}\" RETURN ID(n) as id LIMIT 1";
    private static final String RELATIONSHIP_QUERY = "MATCH ()-[r]-() WHERE r.`{propertyKey}` = \"{keyValue}\" RETURN ID(r) as id LIMIT 1";

    private final String propertyName;
    private final GraphDatabaseService database;

    public PropertyResolver(GraphDatabaseService database, String keyProperty) throws ResolverNotApplicable {
        super(database, keyProperty);
        this.database = database;
        this.propertyName = keyProperty;
    }

    private long resolveProperty(String query, String propertyValue) {
        Map<String, Object> params =new HashMap<>();
        params.put("keyProperty", propertyName);
        params.put("keyValue", propertyValue);

        Result r = database.execute(query, params);
        if (r.hasNext()) {
            Map<String, Object> n = r.next();
            return (long) n.get("id");
        }
        throw new RuntimeException("No item found with '" + propertyName + "'='" + propertyValue + "'");
    }

    @Override
    public long getNodeID(String key) {
        return resolveProperty(NODE_QUERY, key);
    }

    @Override
    public long getRelationshipID(String key) {
        return resolveProperty(RELATIONSHIP_QUERY, key);
    }
}
