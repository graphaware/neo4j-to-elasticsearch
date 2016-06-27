package com.graphaware.module.es.proc.result;

import org.neo4j.graphdb.Node;

public class NodeSearchResult {
    public Node node;
    public Double score;

    public NodeSearchResult() {
    }

    public NodeSearchResult(Node node, Double score) {
        this.node = node;
        this.score = score;
    }
}
