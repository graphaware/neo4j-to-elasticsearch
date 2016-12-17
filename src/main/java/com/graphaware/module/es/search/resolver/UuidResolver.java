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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static com.graphaware.runtime.RuntimeRegistry.getStartedRuntime;

class UuidResolver extends KeyToIdResolver {

    private Object uuidReader;
    private Method nodeUuidToId;
    private Method relUuidToId;

    UuidResolver(final GraphDatabaseService database, final String keyProperty) throws ResolverNotApplicable {
        super(database, keyProperty);
        String uuidProperty;

        checkUuidModuleInstalled();
        try {
            final Object uuidConfig = getUUidConfig(database);
            uuidReader = createUuidReader(database, uuidConfig);
            nodeUuidToId = uuidReader.getClass().getMethod("getNodeIdByUuid", String.class);
            relUuidToId = uuidReader.getClass().getMethod("getRelationshipIdByUuid", String.class);

            // check UUID property
            Method getUuidProperty = getUUIDClass("UuidConfiguration").getMethod("getUuidProperty");
            uuidProperty = (String) getUuidProperty.invoke(uuidConfig);

        } catch (InstantiationException | ClassNotFoundException | NoSuchMethodException | InvocationTargetException |IllegalAccessException e) {
            throw new RuntimeException("Cannot instantiate UuidResolver", e);
        }

        if (!keyProperty.equals(uuidProperty)) {
            throw new ResolverNotApplicable("UUID property (" + uuidProperty + ") and ElasticSearch key property are different");
        }
    }

    private static void checkUuidModuleInstalled() throws ResolverNotApplicable {
        try {
            getUUIDClass("UuidModule");
        } catch (ClassNotFoundException e) {
            throw new ResolverNotApplicable("UUID module not installed");
        }
    }

    private Object getUUidConfig(final GraphDatabaseService database) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method getUuidConfig = getUUIDClass("UuidModule").getMethod("getConfiguration");
        return getUuidConfig.invoke(getStartedRuntime(database).getModule(getUUIDClass("UuidModule")));
    }

    private Object createUuidReader(GraphDatabaseService database, Object uuidConfig) throws NoSuchMethodException, ClassNotFoundException, InvocationTargetException, IllegalAccessException, InstantiationException {
        return getUUIDClass("read.DefaultUuidReader").getConstructor(
                getUUIDClass("UuidConfiguration"),
                org.neo4j.graphdb.GraphDatabaseService.class
        ).newInstance(uuidConfig, database);
    }

    private static Class getUUIDClass(final String name) throws ClassNotFoundException {
        return Class.forName("com.graphaware.module.uuid." + name);
    }

    @Override
    public final long getNodeID(final String key) {
        try {
            return (long) nodeUuidToId.invoke(uuidReader, key);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getRelationshipID(final String key) {
        try {
            return (long) relUuidToId.invoke(uuidReader, key);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
