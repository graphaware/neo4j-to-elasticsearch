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
import com.graphaware.module.es.mapping.expression.NodeExpressions;
import com.graphaware.module.es.mapping.expression.RelationshipExpressions;
import com.graphaware.writer.thirdparty.NodeUpdated;
import com.graphaware.writer.thirdparty.RelationshipUpdated;
import com.graphaware.writer.thirdparty.WriteOperation;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface Mapping {

    Log LOG = LoggerFactory.getLogger(Mapping.class);

    void configure(Map<String, String> config);

    void createIndexAndMapping(JestClient client) throws Exception;

    <T extends Entity> String getIndexFor(Class<T> searchedType);

    String getKeyProperty();

    void setDatabase(GraphDatabaseService database);

    default List<BulkableAction<? extends JestResult>> getActions(WriteOperation<?> operation) {
        switch (operation.getType()) {
            case NODE_CREATED:
                return createNode((NodeExpressions) operation.getDetails());

            case NODE_UPDATED:
                return updateNode(((Change<NodeExpressions>) ((NodeUpdated) operation).getDetails()).getPrevious(), ((Change<NodeExpressions>) ((NodeUpdated) operation).getDetails()).getCurrent());

            case NODE_DELETED:
                return deleteNode((NodeExpressions) operation.getDetails());

            case RELATIONSHIP_CREATED:
                return createRelationship((RelationshipExpressions) operation.getDetails());

            case RELATIONSHIP_UPDATED:
                return updateRelationship(((Change<RelationshipExpressions>) ((RelationshipUpdated) operation).getDetails()).getPrevious(), ((Change<RelationshipExpressions>) ((RelationshipUpdated) operation).getDetails()).getCurrent());

            case RELATIONSHIP_DELETED:
                return deleteRelationship((RelationshipExpressions) operation.getDetails());

            default:
                LOG.warn("Unsupported operation " + operation.getType());
                return Collections.emptyList();
        }
    }

    List<BulkableAction<? extends JestResult>> createNode(NodeExpressions node);

    List<BulkableAction<? extends JestResult>> updateNode(NodeExpressions before, NodeExpressions after);

    List<BulkableAction<? extends JestResult>> deleteNode(NodeExpressions node);

    List<BulkableAction<? extends JestResult>> createRelationship(RelationshipExpressions relationship);

    List<BulkableAction<? extends JestResult>> updateRelationship(RelationshipExpressions before, RelationshipExpressions after);

    List<BulkableAction<? extends JestResult>> deleteRelationship(RelationshipExpressions relationship);

    /**
     * Determines whether or not the concrete Mapping implementation bypass the node inclusion policies.
     * This method is checked during the initialize step of the Elasticsearch module.
     *
     * @return boolean
     */
    boolean bypassInclusionPolicies();

    void reload();
}
