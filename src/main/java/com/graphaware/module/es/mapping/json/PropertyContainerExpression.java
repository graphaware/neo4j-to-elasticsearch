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
package com.graphaware.module.es.mapping.json;

import com.graphaware.common.representation.PropertyContainerRepresentation;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public abstract class PropertyContainerExpression<TPropertyContainerRepresentation extends PropertyContainerRepresentation> {

    private static final String DEFAULT_TIMEZONE = "UTC";

    protected final TPropertyContainerRepresentation propertyContainer;

    public PropertyContainerExpression(TPropertyContainerRepresentation propertyContainer) {
        this.propertyContainer = propertyContainer;
    }

    public boolean hasProperty(String key) {
        return propertyContainer.getProperties().containsKey(key);
    }

    public Object getProperty(String key, Object def) {
        return propertyContainer.getProperties().getOrDefault(key, def);
    }

    public Object getProperty(String key) {
        for (Object s : propertyContainer.getProperties().keySet()) {
            if (s.equals(key)) {
                return propertyContainer.getProperties().get(s);
            }
        }

        return null;
    }

    public abstract String getGraphType();

    public String formatTime(String propertyKey, String format) {
        return formatTime(propertyKey, format, DEFAULT_TIMEZONE);
    }

    public String formatTime(String propertyKey, String format, String timezone) {
        if (!propertyContainer.getProperties().containsKey(propertyKey)) {
            throw new IllegalArgumentException("Node doesn't contains the " + propertyKey + " property");
        }

        Long timestamp = Long.valueOf(propertyContainer.getProperties().get(propertyKey).toString());
        Date date = new Date(timestamp);
        DateFormat dateFormat = new SimpleDateFormat(format);
        dateFormat.setTimeZone(TimeZone.getTimeZone(timezone));

        return dateFormat.format(date);
    }

}
