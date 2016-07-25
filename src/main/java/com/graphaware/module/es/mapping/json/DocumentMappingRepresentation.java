package com.graphaware.module.es.mapping.json;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.graphaware.common.log.LoggerFactory;
import com.graphaware.common.representation.NodeRepresentation;
import com.graphaware.common.representation.RelationshipRepresentation;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestResult;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;

public class DocumentMappingRepresentation {

    private static final Log LOG = LoggerFactory.getLogger(DocumentMappingRepresentation.class);
    
    private static final ObjectMapper om = new ObjectMapper();

    private DocumentMappingDefaults defaults;

    @JsonProperty("node_mappings")
    private List<GraphDocumentMapper> nodeMappers;

    @JsonProperty("relationship_mappings")
    private List<GraphDocumentMapper> relationshipMappers;

    public DocumentMappingDefaults getDefaults() {
        return defaults;
    }

    public List<GraphDocumentMapper> getNodeMappers() {
        return nodeMappers;
    }

    public List<GraphDocumentMapper> getRelationshipMappers() {
        return relationshipMappers;
    }

    public List<BulkableAction<? extends JestResult>> createOrUpdateNode(NodeRepresentation node) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();

        for (GraphDocumentMapper mapper : nodeMappers) {
            if (mapper.supports(node)) {
                try {
                    DocumentRepresentation action = mapper.getDocumentRepresentation(node, defaults);
                    try {
                        String source = om.writeValueAsString(action.getSource());
                        actions.add(new Index.Builder(source).index(action.getIndex()).type(action.getType()).id(action.getId()).build());
                    } catch (IOException ex) {
                        LOG.error("Error while creating json from action: " + node.toString(), ex);
                        // @// TODO: 24/07/16  Should we really throw the exception here, instead of silently logging and failing 
                        //throw new RuntimeException("Error while creating json from action: " + node.toString(), ex);
                    }
                } catch (Exception e) {
                    LOG.error(e.getMessage());
                }


            }
        }

        return actions;
    }

    public List<BulkableAction<? extends JestResult>> createOrUpdateRelationship(RelationshipRepresentation relationship) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();

        for (GraphDocumentMapper mapping : relationshipMappers) {
            if (mapping.supports(relationship)) {
                try {
                    DocumentRepresentation action = mapping.getDocumentRepresentation(relationship, defaults);
                    try {
                        String source = om.writeValueAsString(action.getSource());
                        actions.add(new Index.Builder(source).index(action.getIndex()).type(action.getType()).id(action.getId()).build());
                    } catch (IOException ex) {
                        LOG.error("Error while creating json from action: " + relationship.toString(), ex);
                        // @// TODO: 24/07/16 Same as above
                        // throw new RuntimeException("Error while creating json from action: " + relationship.toString(), ex);
                    }
                } catch (Exception e) {
                    LOG.error(e.getMessage());
                }
            }
        }

        return actions;
    }

    public List<BulkableAction<? extends JestResult>> getDeleteRelationshipActions(RelationshipRepresentation relationship) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();

        for (GraphDocumentMapper mapping : relationshipMappers) {
            if (mapping.supports(relationship)) {
                try {
                    DocumentRepresentation action = mapping.getDocumentRepresentation(relationship, defaults);
                    actions.add(new Delete.Builder(action.getId()).index(action.getIndex()).type(action.getType()).build());
                } catch (Exception e) {
                    LOG.error(e.getMessage());
                }
            }
        }

        return actions;
    }

    public List<BulkableAction<? extends JestResult>> updateNodeAndRemoveOldIndices(NodeRepresentation before, NodeRepresentation after) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();
        List<String> afterIndices = new ArrayList<>();
        for (DocumentRepresentation action : getNodeMappingRepresentations(after, defaults)) {
            afterIndices.add(action.getIndex() + "_" + action.getType());

            try {
                String source = om.writeValueAsString(action.getSource());
                actions.add(new Index.Builder(source).index(action.getIndex()).type(action.getType()).id(action.getId()).build());
            } catch (IOException ex) {
                LOG.error("Error while creating json from action: " + after.toString(), ex);
                throw new RuntimeException("Error while creating json from action: " + after.toString(), ex);
            }
        }

        for (DocumentRepresentation representation : getNodeMappingRepresentations(before, defaults)) {
            if (!afterIndices.contains(representation.getIndex() + "_" + representation.getType())) {
                actions.add(new Delete.Builder(representation.getId()).index(representation.getIndex()).type(representation.getType()).build());
            }
        }

        return actions;
    }

    public List<BulkableAction<? extends JestResult>> updateRelationshipAndRemoveOldIndices(RelationshipRepresentation before, RelationshipRepresentation after) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();
        List<String> afterIndices = new ArrayList<>();

        for (DocumentRepresentation action : getRelationshipMappingRepresentations(after, defaults)) {
            afterIndices.add(action.getIndex() + "_" + action.getType());

            try {
                String source = om.writeValueAsString(action.getSource());
                actions.add(new Index.Builder(source).index(action.getIndex()).type(action.getType()).id(action.getId()).build());
            } catch (IOException e) {
                LOG.error("Error while creating json from action " + after.toString(), e);
            }
        }

        for (DocumentRepresentation representation : getRelationshipMappingRepresentations(before, defaults)) {
            if (!afterIndices.contains(representation.getIndex() + "_" + representation.getType())) {
                actions.add(new Delete.Builder(representation.getId()).index(representation.getIndex()).type(representation.getType()).build());
            }
        }

        return actions;
    }

    public List<BulkableAction<? extends JestResult>> getDeleteNodeActions(NodeRepresentation node) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();
        for (DocumentRepresentation documentRepresentation : getNodeMappingRepresentations(node, defaults)) {
            actions.add(new Delete.Builder(documentRepresentation.getId()).index(documentRepresentation.getIndex()).type(documentRepresentation.getType()).build());
        }

        return actions;
    }

    private List<DocumentRepresentation> getNodeMappingRepresentations(NodeRepresentation nodeRepresentation, DocumentMappingDefaults defaults) {
        List<DocumentRepresentation> docs = new ArrayList<>();
        for (GraphDocumentMapper mapper : getNodeMappers()) {
            if (mapper.supports(nodeRepresentation)) {
                try {
                    DocumentRepresentation representation = mapper.getDocumentRepresentation(nodeRepresentation, defaults);
                    docs.add(representation);
                } catch (Exception e) {
                    LOG.error(e.getMessage());
                }
            }
        }

        return docs;
    }

    private List<DocumentRepresentation> getRelationshipMappingRepresentations(RelationshipRepresentation relationshipRepresentation, DocumentMappingDefaults defaults) {
        List<DocumentRepresentation> docs = new ArrayList<>();
        for (GraphDocumentMapper mapper : getRelationshipMappers()) {
            if (mapper.supports(relationshipRepresentation)) {
                try {
                    DocumentRepresentation representation = mapper.getDocumentRepresentation(relationshipRepresentation, defaults);
                    docs.add(representation);
                } catch (Exception e) {
                    LOG.error(e.getMessage());
                }
            }
        }

        return docs;
    }
}
