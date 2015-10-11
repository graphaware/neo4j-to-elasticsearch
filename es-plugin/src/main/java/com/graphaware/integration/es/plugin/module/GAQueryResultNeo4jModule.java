package com.graphaware.integration.es.plugin.module;

import com.graphaware.integration.es.plugin.query.GAQueryResultNeo4j;
import org.elasticsearch.common.inject.AbstractModule;

public class GAQueryResultNeo4jModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(GAQueryResultNeo4j.class).asEagerSingleton();
    }
}