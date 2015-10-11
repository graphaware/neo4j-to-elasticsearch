package com.graphaware.integration.es.test;

import java.util.Map;

/**
 * An embedded Elasticsearch server for testing.
 */
public interface ElasticSearchServerWrapper {

    void startEmbeddedServer();

    void stopEmbeddedServer();

    void createIndex(String index, Map<String, Object> properties);
}
