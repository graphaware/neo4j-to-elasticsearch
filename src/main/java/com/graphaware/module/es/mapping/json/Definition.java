package com.graphaware.module.es.mapping.json;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.graphaware.common.representation.NodeRepresentation;
import com.graphaware.common.representation.RelationshipRepresentation;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestResult;
import io.searchbox.core.Index;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Definition {

    private static final ObjectMapper om = new ObjectMapper();

    private Defaults defaults;

    @JsonProperty("node_mappings")
    private List<Mapping> nodeMappings;

    @JsonProperty("relationship_mappings")
    private List<Mapping> relationshipMappings;

    public Defaults getDefaults() {
        return defaults;
    }

    public List<Mapping> getNodeMappings() {
        return nodeMappings;
    }

    public List<Mapping> getRelationshipMappings() {
        return relationshipMappings;
    }

    public List<BulkableAction<? extends JestResult>> createOrUpdateNode(NodeRepresentation node) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();

        for (Mapping mapping : nodeMappings) {
            if (mapping.supports(node)) {
                Action action = mapping.getCreateAction(node, defaults);
                if (action.getSource().keySet().size() == 0) {
                    continue;
                }

                try {
                    String source = om.writeValueAsString(action.getSource());
                    actions.add(new Index.Builder(source).index(action.getIndex()).type(action.getType()).id(action.getId()).build());
                } catch (IOException e) {
                    //
                }
            }
        }

        return actions;
    }

    public List<BulkableAction<? extends JestResult>> createOrUpdateRelationship(RelationshipRepresentation relationship) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();

        for (Mapping mapping : relationshipMappings) {
            if (mapping.supports(relationship)) {
                Action action = mapping.getCreateAction(relationship, defaults);
                if (action.getSource().size() == 0) {
                    continue;
                }

                try {
                    String source = om.writeValueAsString(action.getSource());
                    actions.add(new Index.Builder(source).index(action.getIndex()).type(action.getType()).id(action.getId()).build());
                } catch (IOException e) {
                    //
                }
            }
        }

        return actions;
    }
}
