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

import com.graphaware.module.es.ElasticSearchModule;
import com.graphaware.module.es.proc.result.NodeSearchResult;
import com.graphaware.module.es.proc.result.RelationshipSearchResult;
import com.graphaware.module.es.proc.result.StatusResult;
import com.graphaware.module.es.search.Searcher;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

import static com.graphaware.runtime.RuntimeRegistry.getStartedRuntime;

public class ElasticSearchProcedures {

    @Context
    public GraphDatabaseService database;

    private static ElasticSearchModule module;

    private ElasticSearchModule getModule(GraphDatabaseService database) {
        if (module == null) {
            module = getStartedRuntime(database).getModule(ElasticSearchModule.class);
        }
        return module;
    }

    private static Searcher searcher;

    private static Searcher getSearcher(GraphDatabaseService database) {
        if (searcher == null) {
            searcher = new Searcher(database);
        }
        return searcher;
    }

    @Procedure("ga.es.queryRelationship")
    @PerformsWrites
    public Stream<RelationshipSearchResult> queryRelationship(@Name("query") String query) {
        return getSearcher(database).search(query, Relationship.class).stream().map(match -> {
            return new RelationshipSearchResult(match.getItem(), match.score);
        });
    }

    @Procedure("ga.es.queryNode")
    @PerformsWrites
    public Stream<NodeSearchResult> queryNode(@Name("query") String query) {
        return getSearcher(database).search(query, Node.class).stream().map(match -> {
            return new NodeSearchResult(match.getItem(), match.score);
        });
    }

    @Procedure("ga.es.initialized")
    public Stream<StatusResult> initialized() {
        return Stream.of(new StatusResult(getModule(database).isReindexCompleted()));
    }
}

