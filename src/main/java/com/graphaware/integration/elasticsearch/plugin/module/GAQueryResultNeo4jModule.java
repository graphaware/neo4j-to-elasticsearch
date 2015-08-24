package com.graphaware.integration.elasticsearch.plugin.module;

import com.graphaware.integration.elasticsearch.plugin.query.GAQueryResultNeo4j;
import org.elasticsearch.common.inject.AbstractModule;

public class GAQueryResultNeo4jModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(GAQueryResultNeo4j.class).asEagerSingleton();
    }
}