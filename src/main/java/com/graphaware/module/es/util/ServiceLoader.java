/*
 * Copyright (c) 2013-2016 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.es.util;

import com.graphaware.module.es.mapping.Mapping;

public class ServiceLoader {

    public static Mapping loadMapping(String mappingClass) {
        try {
            Class<?> clazz;
            try {
                clazz = Class.forName(mappingClass);
            } catch(ClassNotFoundException e) {
                if (!mappingClass.contains(".")) {
                    /**
                     * If the mapping class name does not contain a ".", assume that is is the name of
                     * a class in the same package as the Mapping interface and try again.
                     */
                    clazz = Class.forName(Mapping.class.getPackage().getName() + "." + mappingClass);
                } else {
                    throw e;
                }
            }

            Object definition = clazz.newInstance();
            if (definition instanceof Mapping) {
                return (Mapping) definition;
            } else {
                throw new IllegalArgumentException(mappingClass + " is not a Mapping class");
            }

        } catch (ClassNotFoundException
                | InstantiationException
                | IllegalAccessException
                | IllegalArgumentException e) {
            throw new RuntimeException("Could not instantiate mapping class " + mappingClass + " : " + e.getMessage(), e);
        }
    }
}
