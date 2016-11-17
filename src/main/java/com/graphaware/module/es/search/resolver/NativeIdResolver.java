package com.graphaware.module.es.search.resolver;

import com.graphaware.module.es.mapping.BaseMapping;
import org.neo4j.graphdb.GraphDatabaseService;

class NativeIdResolver extends KeyToIdResolver {

    NativeIdResolver(GraphDatabaseService database, String keyProperty) throws ResolverNotApplicable {
        super(database, keyProperty);

        if (!BaseMapping.NATIVE_ID.equals(keyProperty)) {
            throw new ResolverNotApplicable("key property is not the native Neo4j identifier");
        }
    }

    @Override
    public long getNodeID(String key) {
        return Long.parseLong(key);
    }

    @Override
    public long getRelationshipID(String key) {
        return Long.parseLong(key);
    }
}
