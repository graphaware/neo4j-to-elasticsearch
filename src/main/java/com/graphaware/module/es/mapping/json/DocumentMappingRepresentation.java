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
                DocumentRepresentation action = mapper.getDocumentRepresentation(node, defaults);
                if (action.getSource().keySet().isEmpty()) {
                    continue;
                }

                try {
                    String source = om.writeValueAsString(action.getSource());
                    actions.add(new Index.Builder(source).index(action.getIndex()).type(action.getType()).id(action.getId()).build());
                } catch (IOException ex) {
                    LOG.error("Error while creating json from action: " + node.toString(), ex);
                    throw new RuntimeException("Error while creating json from action: " + node.toString(), ex);
                }                
            }
        }

        return actions;
    }

    public List<BulkableAction<? extends JestResult>> createOrUpdateRelationship(RelationshipRepresentation relationship) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();

        for (GraphDocumentMapper mapping : relationshipMappers) {
            if (mapping.supports(relationship)) {
                DocumentRepresentation action = mapping.getDocumentRepresentation(relationship, defaults);
                if (action.getSource().isEmpty()) {
                    continue;
                }
                try {
                    String source = om.writeValueAsString(action.getSource());
                    actions.add(new Index.Builder(source).index(action.getIndex()).type(action.getType()).id(action.getId()).build());
                } catch (IOException ex) {
                    LOG.error("Error while creating json from action: " + relationship.toString(), ex);
                    throw new RuntimeException("Error while creating json from action: " + relationship.toString(), ex);
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
            if (action.getSource().keySet().isEmpty()) {
                continue;
            }

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

    private List<DocumentRepresentation> getNodeMappingRepresentations(NodeRepresentation nodeRepresentation, DocumentMappingDefaults defaults) {
        List<DocumentRepresentation> docs = new ArrayList<>();
        for (GraphDocumentMapper mapper : getNodeMappers()) {
            if (mapper.supports(nodeRepresentation)) {
                DocumentRepresentation representation = mapper.getDocumentRepresentation(nodeRepresentation, defaults);
                docs.add(representation);
            }
        }

        return docs;
    }
}
