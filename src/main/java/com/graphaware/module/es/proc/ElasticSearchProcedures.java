/*
 * Copyright (c) 2013-2016 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.es.proc;

import com.graphaware.common.log.LoggerFactory;
import com.graphaware.module.es.ElasticSearchModule;
import com.graphaware.module.es.proc.result.*;
import com.graphaware.module.es.search.Searcher;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.stream.Stream;

import static com.graphaware.runtime.RuntimeRegistry.getStartedRuntime;

public class ElasticSearchProcedures {

    private static final Log LOG = LoggerFactory.getLogger(ElasticSearchProcedures.class);

    private static ThreadLocal<Searcher> searcherCache = new ThreadLocal<>();

    @Context
    public GraphDatabaseService database;

    private ElasticSearchModule getModule(GraphDatabaseService database) {
        return getStartedRuntime(database).getModule(ElasticSearchModule.class);
    }

    /**
     * Needed to reset searcher cache during tests.
     */
    static void resetSearcherCache() {
        searcherCache = new ThreadLocal<>();
    }

    private static Searcher getSearcher(GraphDatabaseService database) {
        if (searcherCache.get() == null) {
            searcherCache.set(new Searcher(database));
        }
        return searcherCache.get();
    }

    @Procedure(value = "ga.es.queryNode", mode = Mode.WRITE)
    public Stream<NodeSearchResult> queryNode(@Name("query") String query) {
        try {
            return getSearcher(database).search(query, Node.class).stream().map(match -> {
                return new NodeSearchResult(match.getItem(), match.score);
            });
        } catch (Exception e) {
            LOG.error("", e);

            return Stream.empty();
        }
    }

    @Procedure(value = "ga.es.queryRelationship", mode = Mode.WRITE)
    public Stream<RelationshipSearchResult> queryRelationship(@Name("query") String query) {
        return getSearcher(database).search(query, Relationship.class).stream().map(match -> {
            return new RelationshipSearchResult(match.getItem(), match.score);
        });
    }

    @Procedure(value = "ga.es.queryNodeRaw", mode = Mode.WRITE)
    public Stream<JsonSearchResult> queryNodeRaw(@Name("query") String query) {
        return Stream.of(new JsonSearchResult(getSearcher(database).rawSearch(query, Node.class)));
    }

    @Procedure(value = "ga.es.queryRelationshipRaw", mode = Mode.WRITE)
    public Stream<JsonSearchResult> queryRelationshipRaw(@Name("query") String query) {
        return Stream.of(new JsonSearchResult(getSearcher(database).rawSearch(query, Relationship.class)));
    }

    @Procedure("ga.es.nodeMapping")
    public Stream<JsonSearchResult> nodeMapping() {
        return Stream.of(new JsonSearchResult(getSearcher(database).nodeMapping()));
    }

    @Procedure("ga.es.relationshipMapping")
    public Stream<JsonSearchResult> relationshipMapping() {
        return Stream.of(new JsonSearchResult(getSearcher(database).relationshipMapping()));
    }

    @Procedure("ga.es.initialized")
    public Stream<StatusResult> initialized() {
        return Stream.of(new StatusResult(getModule(database).isReindexCompleted()));
    }

    @Procedure("ga.es.info")
    public Stream<JsonSearchResult> info() {
        return Stream.of(new JsonSearchResult(getSearcher(database).getEsInfo()));
    }

    @Procedure("ga.es.reloadMapping")
    public Stream<SingleResult> reloadMapping() {
        getStartedRuntime(database).getModule(ElasticSearchModule.class).getWriter().reloadMapping();
        return Stream.of(SingleResult.success());
    }
}

