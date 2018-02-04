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

package com.graphaware.module.es.mapping.expression;

import com.graphaware.common.expression.EntityExpressions;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public interface ConvertingEntityExpressions extends EntityExpressions {

    String DEFAULT_TIMEZONE = "UTC";

    String getGraphType();

    default String formatTime(String propertyKey, String format) {
        return formatTime(propertyKey, format, DEFAULT_TIMEZONE);
    }

    default String formatTime(String propertyKey, String format, String timezone) {
        if (!hasProperty(propertyKey)) {
            throw new IllegalArgumentException("Node doesn't contains the " + propertyKey + " property");
        }

        Long timestamp = Long.valueOf(getProperty(propertyKey).toString());
        Date date = new Date(timestamp);
        DateFormat dateFormat = new SimpleDateFormat(format);
        dateFormat.setTimeZone(TimeZone.getTimeZone(timezone));

        return dateFormat.format(date);
    }

    default String asString(String key) {
        if (hasProperty(key)) {
            return getProperty(key).toString();
        }

        return null;
    }

    default Long asLong(String key) {
        if (hasProperty(key)) {
            return Long.valueOf(getProperty(key).toString());
        }

        return null;
    }

    default Integer asInt(String key) {
        if (hasProperty(key)) {
            return Integer.parseInt(getProperty(key).toString());
        }

        return null;
    }

    default Float asFloat(String key) {
        if (hasProperty(key)) {
            return Float.valueOf(getProperty(key).toString());
        }

        return null;
    }

    default Double asDouble(String key) {
        if (hasProperty(key)) {
            return Double.valueOf(getProperty(key).toString());
        }

        return null;
    }
}
