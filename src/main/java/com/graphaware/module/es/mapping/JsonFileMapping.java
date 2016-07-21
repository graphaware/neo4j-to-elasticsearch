package com.graphaware.module.es.mapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.graphaware.common.representation.NodeRepresentation;
import com.graphaware.common.representation.RelationshipRepresentation;
import com.graphaware.module.es.mapping.json.DocumentMappingRepresentation;
import com.graphaware.writer.thirdparty.*;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import org.neo4j.graphdb.PropertyContainer;
import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JsonFileMapping implements Mapping {

    private static final String DEFAULT_KEY_PROPERTY = "uuid";

    private static final String FILE_PATH_KEY = "file";
    private DocumentMappingRepresentation mappingRepresentation;

    protected String keyProperty;

    @Override
    public void configure(Map<String, String> config) {
        if (!config.containsKey(FILE_PATH_KEY)) {
            throw new RuntimeException("Configuration is missing the " + FILE_PATH_KEY + "key");
        }
        try {
            String file = new ClassPathResource(config.get("file")).getFile().getAbsolutePath();
            mappingRepresentation = new ObjectMapper().readValue(new File(file), DocumentMappingRepresentation.class);
        } catch (IOException e) {
            throw new RuntimeException("Unable to read json mapping file", e);
        }
    }

    public DocumentMappingRepresentation getMappingRepresentation() {
        return mappingRepresentation;
    }

    @Override
    public List<BulkableAction<? extends JestResult>> getActions(WriteOperation operation) {
        switch (operation.getType()) {
            case NODE_CREATED:
                return createNode(((NodeCreated) operation).getDetails());

            case RELATIONSHIP_CREATED:
                return createRelationship(((RelationshipCreated) operation).getDetails());
            /*

            case NODE_UPDATED:
                NodeUpdated nodeUpdated = (NodeUpdated) operation;
                return updateNode(nodeUpdated.getDetails().getPrevious(), nodeUpdated.getDetails().getCurrent());

            case NODE_DELETED:
                return deleteNode(((NodeDeleted) operation).getDetails());



            case RELATIONSHIP_UPDATED:
                RelationshipUpdated relUpdated = (RelationshipUpdated) operation;
                return updateRelationship(relUpdated.getDetails().getPrevious(), relUpdated.getDetails().getCurrent());

            case RELATIONSHIP_DELETED:
                return deleteRelationship(((RelationshipDeleted) operation).getDetails());
                */

            default:
                //LOG.warn("Unsupported operation " + operation.getType());
                return Collections.emptyList();
        }
    }

    protected List<BulkableAction<? extends JestResult>> createNode(NodeRepresentation node) {
        return mappingRepresentation.createOrUpdateNode(node);
    }
    
    protected List<BulkableAction<? extends JestResult>> createRelationship(RelationshipRepresentation relationship) {
        return mappingRepresentation.createOrUpdateRelationship(relationship);
    }

    @Override
    public void createIndexAndMapping(JestClient client) throws Exception {

    }

    @Override
    public <T extends PropertyContainer> String getIndexFor(Class<T> searchedType) {
        return null;
    }

    @Override
    public String getKeyProperty() {
        return mappingRepresentation.getDefaults().getKeyProperty() != null ? mappingRepresentation.getDefaults().getKeyProperty() : DEFAULT_KEY_PROPERTY;
    }
}
