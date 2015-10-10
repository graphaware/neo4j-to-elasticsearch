package com.graphaware.elasticsearch.wrapper;

import java.util.Map;

public interface EmbeddedServerWrapper {

    void startEmbeddedServer();

    void stopEmbeddedServer();

    void createIndex(String index, Map<String, Object> properties);
}
