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

package com.graphaware.module.es.search;

import org.neo4j.graphdb.GraphDatabaseService;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static com.graphaware.runtime.RuntimeRegistry.getStartedRuntime;

public class Key2IdResolver {
    private final Object uuidReader;
    private final Method nodeUuidToId;
    private final Method relUuidToId;

    public Key2IdResolver(final GraphDatabaseService database, final String keyProperty) {

        if (keyProperty.equals("ID()")) {
            uuidReader = null;
            nodeUuidToId = null;
            relUuidToId = null;
            return;
        }

        // instantiate the UuidReader (if needed)
        try {
            uuidReader = createUuidReader(database);
            nodeUuidToId = uuidReader.getClass().getMethod("getNodeIdByUuid", String.class);
            relUuidToId = uuidReader.getClass().getMethod("getRelationshipIdByUuid", String.class);
        } catch (Exception e) {
            throw new RuntimeException("Could not instantiate UuidReader", e);
        }
    }

    // createUuidReader(database)
    private Object createUuidReader(GraphDatabaseService database) throws NoSuchMethodException, ClassNotFoundException, InvocationTargetException, IllegalAccessException, InstantiationException {
        Method getUuidConfig = getUUIDClass("UuidModule").getMethod("getConfiguration");

        return getUUIDClass("read.DefaultUuidReader").getConstructor(
                getUUIDClass("UuidConfiguration"),
                org.neo4j.graphdb.GraphDatabaseService.class
        ).newInstance(
                getUuidConfig.invoke(getStartedRuntime(database).getModule(getUUIDClass("UuidModule"))),
                database
        );
    }

    private Class getUUIDClass(final String name) throws ClassNotFoundException {
        return Class.forName("com.graphaware.module.uuid." + name);
    }

    /**
     * Resolve the Neo4j ID from the ElasticSearch ID.
     *
     * @param key Value if the "_id" field in ElasticSearch, extracted using {@link com.graphaware.module.es.mapping.BaseMapping#getKey}
     * @return the ID of the node
     */
    public final long getNodeID(final String key) {
        if (uuidReader == null) {
            return Long.parseLong(key);
        } else {
            try {
                return (long) nodeUuidToId.invoke(uuidReader, key);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Resolve the Neo4j ID from the ElasticSearch ID.
     *
     * @param key Value if the "_id" field in ElasticSearch, extracted using {@link com.graphaware.module.es.mapping.BaseMapping#getKey}
     * @return the ID of the relationship
     */
    public long getRelationshipID(final String key) {
        if (uuidReader == null) {
            return Long.parseLong(key);
        } else {
            try {
                return (long) relUuidToId.invoke(uuidReader, key);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
