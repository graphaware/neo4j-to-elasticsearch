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

import com.graphaware.module.es.mapping.BaseMapping;
import org.neo4j.graphdb.GraphDatabaseService;

class NativeIdResolver extends KeyToIdResolver {

    NativeIdResolver(GraphDatabaseService database, String keyProperty) throws ResolverNotApplicable {
        super(database, keyProperty);

        if (!BaseMapping.NATIVE_ID.equals(keyProperty)) {
            throw new ResolverNotApplicable("key property is not the native Neo4j identifier");
        }
    }

    @Override
    public long getNodeID(String key) {
        return Long.parseLong(key);
    }

    @Override
    public long getRelationshipID(String key) {
        return Long.parseLong(key);
    }
}
