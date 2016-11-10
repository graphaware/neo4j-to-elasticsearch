package com.graphaware.module.es.mapping;

import org.neo4j.graphdb.GraphDatabaseService;

public interface TransactionAwareMapping {

    public void setGraphDatabaseService(GraphDatabaseService database);

}
