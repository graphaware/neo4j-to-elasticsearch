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
import com.graphaware.common.representation.NodeRepresentation;
import com.graphaware.common.representation.RelationshipRepresentation;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestResult;
import io.searchbox.core.Index;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;

import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

/**
 * This mapping indexes all documents in the same ElasticSearch index.
 *
 * The node's neo4j labels are stored are ElasticSearch "type".
 * If a node has multiple labels, it is stored multiple times, once for each label.
 *
 * Relationships are not indexed.
 */
public class AdvancedMapping extends DefaultMapping {
    private static final Log LOG = LoggerFactory.getLogger(AdvancedMapping.class);
    public static final String NODE_TYPE = "node";
    public static final String RELATIONSHIP_TYPE = "relationship";
    public static final String LABELS_FIELD = "_labels";
    public static final String RELATIONSHIP_FIELD = "_relationship";

    @Override
    protected List<BulkableAction<? extends JestResult>> createOrUpdateNode(NodeRepresentation node) {
        String id = getKey(node);
        String source = getJson(node);
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();
        actions.add(new Index.Builder(source).index(getIndexFor(Node.class)).type(NODE_TYPE).id(id).build());
        return actions;
    }

    @Override
    protected List<BulkableAction<? extends JestResult>> createOrUpdateRelationship(RelationshipRepresentation r) {
        return Collections.singletonList(
                new Index.Builder(getJson(r)).index(getIndexFor(Relationship.class)).type(RELATIONSHIP_TYPE).id(getKey(r)).build()
        );
    }
     
    protected void addExtra(Map<String, Object> data, NodeRepresentation node) {
        data.put(LABELS_FIELD,  new ArrayList<>(Arrays.asList(node.getLabels())));
    }
    
    protected void addExtra(Map<String, Object> data, RelationshipRepresentation relationship) {
        data.put(RELATIONSHIP_FIELD,  new ArrayList<>(Arrays.asList(relationship.getType())));
    }

}
