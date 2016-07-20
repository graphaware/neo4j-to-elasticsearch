package com.graphaware.module.es.mapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.graphaware.module.es.mapping.json.Definition;
import com.graphaware.writer.thirdparty.WriteOperation;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import org.neo4j.graphdb.PropertyContainer;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class JsonFileMapping implements Mapping {

    private static final String DEFAULT_ENCODING = "UTF-8";
    private static final String DEFAULT_KEY_PROPERTY = "uuid";

    private static final String FILE_PATH_KEY = "file";
    private Definition definition;

    protected String keyProperty;

    @Override
    public void configure(Map<String, String> config) {
        if (!config.containsKey(FILE_PATH_KEY)) {
            throw new RuntimeException("Configuration is missing the " + FILE_PATH_KEY + "key");
        }
        try {
            String file = new ClassPathResource(config.get("file")).getFile().getAbsolutePath();
            byte[] encoded = Files.readAllBytes(Paths.get(file));
            String json = new String(encoded, DEFAULT_ENCODING);
            definition = new ObjectMapper().readValue(json, Definition.class);
            System.out.println(json);
            System.out.println(definition);
        } catch (IOException e) {
            throw new RuntimeException("Unable to read json mapping file", e);
        }
    }

    @Override
    public List<BulkableAction<? extends JestResult>> getActions(WriteOperation operation) {
        return null;
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
        return keyProperty != null ? keyProperty : DEFAULT_KEY_PROPERTY;
    }
}
