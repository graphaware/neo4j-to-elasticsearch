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
import com.graphaware.common.util.Change;
import com.graphaware.module.es.mapping.json.NodeRepresentation;
import com.graphaware.module.es.mapping.json.RelationshipRepresentation;
import com.graphaware.writer.thirdparty.NodeUpdated;
import com.graphaware.writer.thirdparty.RelationshipUpdated;
import com.graphaware.writer.thirdparty.WriteOperation;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.logging.Log;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface Mapping {

    Log LOG = LoggerFactory.getLogger(Mapping.class);

    void configure(Map<String, String> config);

    void createIndexAndMapping(JestClient client) throws Exception;

    <T extends PropertyContainer> String getIndexFor(Class<T> searchedType);

    String getKeyProperty();

    default List<BulkableAction<? extends JestResult>> getActions(WriteOperation<?> operation) {
        switch (operation.getType()) {
            case NODE_CREATED:
                return createNode((NodeRepresentation) operation.getDetails());

            case NODE_UPDATED:
                NodeUpdated nodeUpdated = (NodeUpdated) operation;
                Change<NodeRepresentation> details = (Change<NodeRepresentation>) nodeUpdated.getDetails();
                return updateNode(details.getPrevious(), details.getCurrent());

            case NODE_DELETED:
                return deleteNode((NodeRepresentation) operation.getDetails());

            case RELATIONSHIP_CREATED:
                return createRelationship((RelationshipRepresentation) operation.getDetails());

            case RELATIONSHIP_UPDATED:
                RelationshipUpdated relUpdated = (RelationshipUpdated) operation;
                Change<RelationshipRepresentation> details1 = (Change<RelationshipRepresentation>) relUpdated.getDetails();
                return updateRelationship(details1.getPrevious(), details1.getCurrent());

            case RELATIONSHIP_DELETED:
                return deleteRelationship((RelationshipRepresentation) operation.getDetails());

            default:
                LOG.warn("Unsupported operation " + operation.getType());
                return Collections.emptyList();
        }
    }

    List<BulkableAction<? extends JestResult>> createNode(NodeRepresentation node);

    List<BulkableAction<? extends JestResult>> updateNode(NodeRepresentation before, NodeRepresentation after);

    List<BulkableAction<? extends JestResult>> deleteNode(NodeRepresentation node);

    List<BulkableAction<? extends JestResult>> createRelationship(RelationshipRepresentation relationship);

    List<BulkableAction<? extends JestResult>> updateRelationship(RelationshipRepresentation before, RelationshipRepresentation after);

    List<BulkableAction<? extends JestResult>> deleteRelationship(RelationshipRepresentation relationship);

}
