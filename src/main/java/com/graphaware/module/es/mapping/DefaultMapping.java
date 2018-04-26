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

package com.graphaware.module.es.mapping;

import com.graphaware.common.log.LoggerFactory;
import com.graphaware.module.es.mapping.expression.NodeExpressions;
import com.graphaware.module.es.mapping.expression.RelationshipExpressions;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestResult;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;

import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Map;

/**
 * This mapping indexes all documents in the same ElasticSearch index.
 *
 * The node's neo4j labels are stored are ElasticSearch "type".
 * If a node has multiple labels, it is stored multiple times, once for each label.
 *
 * Relationships are not indexed.
 */
public class DefaultMapping extends BaseMapping implements Mapping {

    private static final Log LOG = LoggerFactory.getLogger(DefaultMapping.class);

    public DefaultMapping() {

    }

    @Override
    public List<BulkableAction<? extends JestResult>> deleteNode(NodeExpressions node) {
        String id = getKey(node);
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();

        for (String label : node.getLabels()) {
            actions.add(new Delete.Builder(id).index(getIndexFor(Node.class)).type(label).build());
        }

        return actions;
    }

    public static DefaultMapping newInstance() {
        return new DefaultMapping();
    }

    @Override
    public List<BulkableAction<? extends JestResult>> updateNode(NodeExpressions before, NodeExpressions after) {
        return createOrUpdateNode(after);
    }

    @Override
    public List<BulkableAction<? extends JestResult>> createNode(NodeExpressions node) {
        return createOrUpdateNode(node);
    }

    protected List<BulkableAction<? extends JestResult>> createOrUpdateNode(NodeExpressions node) {
        String id = getKey(node);
        Map<String, Object> source = map(node);
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();

        for (String label : node.getLabels()) {
            actions.add(new Index.Builder(source).index(getIndexFor(Node.class)).type(label).id(id).build());
        }

        return actions;
    }

    @Override
    public List<BulkableAction<? extends JestResult>> createRelationship(RelationshipExpressions relationship) {
        return createOrUpdateRelationship(relationship);
    }

    @Override
    public List<BulkableAction<? extends JestResult>> updateRelationship(RelationshipExpressions before, RelationshipExpressions after) {
        return createOrUpdateRelationship(after);
    }

    @Override
    public List<BulkableAction<? extends JestResult>> deleteRelationship(RelationshipExpressions r) {
        return Collections.singletonList(
                new Delete.Builder(getKey(r)).index(getIndexFor(Relationship.class)).type(r.getType()).build()
        );
    }

    protected List<BulkableAction<? extends JestResult>> createOrUpdateRelationship(RelationshipExpressions r) {
        return Collections.singletonList(
                new Index.Builder(map(r)).index(getIndexFor(Relationship.class)).type(r.getType()).id(getKey(r)).build()
        );
    }

    @Override
    public <T extends Entity> String getIndexFor(Class<T> searchedType) {
        return getIndexPrefix() + (searchedType.equals(Node.class) ? "-node" : "-relationship");
    }

    @Override
    public void reload() {

    }
}
