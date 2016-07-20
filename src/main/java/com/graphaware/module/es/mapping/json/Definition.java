package com.graphaware.module.es.mapping.json;


import com.graphaware.common.representation.NodeRepresentation;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestResult;
import io.searchbox.core.Index;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Definition {

    private static final ObjectMapper om = new ObjectMapper();

    private Defaults defaults;

    private List<Mapping> mappings;

    public Defaults getDefaults() {
        return defaults;
    }

    public List<Mapping> getMappings() {
        return mappings;
    }

    public List<BulkableAction<? extends JestResult>> createOrUpdateNode(NodeRepresentation node) {
        List<BulkableAction<? extends JestResult>> actions = new ArrayList<>();

        for (Mapping mapping : mappings) {
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
}
