package com.graphaware.module.es.mapping;

import com.graphaware.writer.thirdparty.WriteOperation;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import org.neo4j.graphdb.PropertyContainer;

import java.util.List;
import java.util.Map;

public interface MappingDefinition {

    void configure(Map<String, String> config);

    List<BulkableAction<? extends JestResult>> getActions(WriteOperation operation);

    void createIndexAndMapping(JestClient client) throws Exception;

    <T extends PropertyContainer> String getIndexFor(Class<T> searchedType);

}
