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


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.graphaware.common.log.LoggerFactory;
import com.graphaware.module.es.mapping.expression.NodeExpressions;
import com.graphaware.module.es.mapping.expression.RelationshipExpressions;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestResult;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;

public class DocumentMappingRepresentation {

    private static final Log LOG = LoggerFactory.getLogger(DocumentMappingRepresentation.class);

    private static final ObjectMapper objectMapper;

    static {
        ObjectMapper om = new ObjectMapper();
        om.registerModule(new AfterburnerModule());
        objectMapper = om;
    }
    
    private DocumentMappingDefaults defaults;

    @JsonProperty("node_mappings")
    private List<GraphDocumentMapper> nodeMappers;

    @JsonProperty("relationship_mappings")
    private List<GraphDocumentMapper> relationshipMappers;

    @JsonIgnore
    private GraphDatabaseService database;

    public DocumentMappingDefaults getDefaults() {
        return defaults;
    }

    public List<GraphDocumentMapper> getNodeMappers() {
        return nodeMappers;
    }

    public List<GraphDocumentMapper> getRelationshipMappers() {
        return relationshipMappers;
    }

    public List<BulkableAction<? extends JestResult>> createOrUpdateNode(NodeExpressions node) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();

        for (GraphDocumentMapper mapper : nodeMappers) {
            if (mapper.supports(node)) {
                try {
                    DocumentRepresentation document = mapper.getDocumentRepresentation(node, defaults, database);
                    String json = objectMapper.writeValueAsString(document);
                    actions.add(new Index.Builder(json).index(document.getIndex()).type(document.getType()).id(document.getId()).build());
                } catch (Exception e) {
                    LOG.error("Error while creating or updating node", e);
                }

            }
        }

        return actions;
    }

    public List<BulkableAction<? extends JestResult>> createOrUpdateRelationship(RelationshipExpressions relationship) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();

        for (GraphDocumentMapper mapping : relationshipMappers) {
            if (mapping.supports(relationship)) {
                try {
                    DocumentRepresentation document = mapping.getDocumentRepresentation(relationship, defaults);
                    String json = objectMapper.writeValueAsString(document);
                    actions.add(new Index.Builder(json).index(document.getIndex()).type(document.getType()).id(document.getId()).build());
                } catch (Exception e) {
                    LOG.error("Error while creating relationship: " + relationship.toString(), e);
                }
            }
        }

        return actions;
    }

    public List<BulkableAction<? extends JestResult>> getDeleteRelationshipActions(RelationshipExpressions relationship) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();

        for (GraphDocumentMapper mapping : relationshipMappers) {
            if (mapping.supports(relationship)) {
                try {
                    DocumentRepresentation document = mapping.getDocumentRepresentation(relationship, defaults);
                    actions.add(new Delete.Builder(document.getId()).index(document.getIndex()).type(document.getType()).build());
                } catch (Exception e) {
                    LOG.error("Error while deleting relationship: " + relationship.toString(), e);
                }
            }
        }

        return actions;
    }

    public List<BulkableAction<? extends JestResult>> updateNodeAndRemoveOldIndices(NodeExpressions before, NodeExpressions after) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();
        List<String> afterIndices = new ArrayList<>();
        for (DocumentRepresentation action : getNodeMappingRepresentations(after, defaults)) {
            afterIndices.add(action.getIndex() + "_" + action.getType());
            try {
                String json = objectMapper.writeValueAsString(action);
                actions.add(new Index.Builder(json).index(action.getIndex()).type(action.getType()).id(action.getId()).build());
            } catch (Exception ex) {
                LOG.error("Error while adding action for node: " + before.toString(), ex);
            }
        }

        for (DocumentRepresentation representation : getNodeMappingRepresentations(before, defaults)) {
            if (!afterIndices.contains(representation.getIndex() + "_" + representation.getType())) {
                actions.add(new Delete.Builder(representation.getId()).index(representation.getIndex()).type(representation.getType()).build());
            }
        }

        return actions;
    }

    public List<BulkableAction<? extends JestResult>> updateRelationshipAndRemoveOldIndices(RelationshipExpressions before, RelationshipExpressions after) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();
        List<String> afterIndices = new ArrayList<>();

        for (DocumentRepresentation action : getRelationshipMappingRepresentations(after, defaults)) {
            afterIndices.add(action.getIndex() + "_" + action.getType());
            try {
                String json = objectMapper.writeValueAsString(action);
                actions.add(new Index.Builder(json).index(action.getIndex()).type(action.getType()).id(action.getId()).build());
            } catch (Exception ex) {
                LOG.error("Error while adding update action for nodes: " + before.toString() + " -> " + after.toString(), ex);
            }            
        }

        for (DocumentRepresentation representation : getRelationshipMappingRepresentations(before, defaults)) {
            if (!afterIndices.contains(representation.getIndex() + "_" + representation.getType())) {
                actions.add(new Delete.Builder(representation.getId()).index(representation.getIndex()).type(representation.getType()).build());
            }
        }

        return actions;
    }

    public List<BulkableAction<? extends JestResult>> getDeleteNodeActions(NodeExpressions node) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();
        for (DocumentRepresentation documentRepresentation : getNodeMappingRepresentations(node, defaults)) {
            actions.add(new Delete.Builder(documentRepresentation.getId()).index(documentRepresentation.getIndex()).type(documentRepresentation.getType()).build());
        }

        return actions;
    }

    public void setDatabase(GraphDatabaseService graphDatabaseService) {
        this.database = graphDatabaseService;
    }

    private List<DocumentRepresentation> getNodeMappingRepresentations(NodeExpressions nodeExpressions, DocumentMappingDefaults defaults) {
        List<DocumentRepresentation> docs = new ArrayList<>();
        for (GraphDocumentMapper mapper : getNodeMappers()) {
            if (mapper.supports(nodeExpressions)) {
                try {
                    DocumentRepresentation document = mapper.getDocumentRepresentation(nodeExpressions, defaults, database);
                    docs.add(document);
                } catch (Exception e) {
                    LOG.error("Error while getting document for node: " + nodeExpressions.toString(), e);
                }
            }
        }

        return docs;
    }

    private List<DocumentRepresentation> getRelationshipMappingRepresentations(RelationshipExpressions relationshipExpressions, DocumentMappingDefaults defaults) {
        List<DocumentRepresentation> docs = new ArrayList<>();
        for (GraphDocumentMapper mapper : getRelationshipMappers()) {
            if (mapper.supports(relationshipExpressions)) {
                try {
                    DocumentRepresentation document = mapper.getDocumentRepresentation(relationshipExpressions, defaults);
                    docs.add(document);
                } catch (Exception e) {
                    LOG.error("Error while getting document for relationship: " + relationshipExpressions.toString(), e);
                }
            }
        }

        return docs;
    }
}
