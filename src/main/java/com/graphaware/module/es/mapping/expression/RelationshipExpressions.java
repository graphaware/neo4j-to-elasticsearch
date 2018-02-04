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

import com.graphaware.common.representation.GraphDetachedRelationship;
import org.neo4j.graphdb.Relationship;

public class RelationshipExpressions extends GraphDetachedRelationship implements ConvertingEntityExpressions {

    private static final String GRAPH_TYPE_RELATIONSHIP = "relationship";

    public RelationshipExpressions(Relationship relationship) {
        super(relationship);
    }

    public RelationshipExpressions(Relationship relationship, String[] properties) {
        super(relationship, properties);
    }

    public boolean allRelationships() {
        return true;
    }

    public String getGraphType() {
        return GRAPH_TYPE_RELATIONSHIP;
    }
}
