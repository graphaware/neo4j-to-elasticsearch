package com.graphaware.module.es.proc.result;

import org.neo4j.graphdb.Relationship;

public class RelationshipSearchResult {
    public Relationship relationship;
    public Double score;

    public RelationshipSearchResult() {
    }

    public RelationshipSearchResult(Relationship relationship, Double score) {
        this.relationship = relationship;
        this.score = score;
    }
}
