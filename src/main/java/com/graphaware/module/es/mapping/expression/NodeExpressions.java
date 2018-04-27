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

import com.graphaware.common.representation.GraphDetachedNode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class NodeExpressions extends GraphDetachedNode implements ConvertingEntityExpressions {

    private static final String GRAPH_TYPE_NODE = "node";

    public NodeExpressions(Node node) {
        super(node);
    }

    public NodeExpressions(Node node, String[] properties) {
        super(node, properties);
    }

    public boolean allNodes() {
        return true;
    }

    public String getGraphType() {
        return GRAPH_TYPE_NODE;
    }

    public QueryExpression query(String query) {
        return new QueryExpression(query);
    }
}
