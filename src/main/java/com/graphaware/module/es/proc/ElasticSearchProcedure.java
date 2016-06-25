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
import static com.graphaware.runtime.RuntimeRegistry.getStartedRuntime;

import com.graphaware.module.es.search.SearchMatch;
import com.graphaware.module.es.search.Searcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.ProcedureSignature;

import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

public class ElasticSearchProcedure {

    private static final String PARAMETER_NAME_INPUT = "input";
    private static final String PARAMETER_NAME_QUERY = "query";
    private static final String PARAMETER_NAME_OUTPUT_NODE = "node";
    private static final String PARAMETER_NAME_OUTPUT_RELATIONSHIP = "relationship";
    private static final String PARAMETER_NAME_SCORE = "score";
    private static final String PARAMETER_NAME_STATUS = "status";

    private final GraphDatabaseService database;
    private final Searcher searcher;

    public ElasticSearchProcedure(GraphDatabaseService database) {
        this.database = database;
        this.searcher = new Searcher(database);
    }

    public CallableProcedure.BasicProcedure queryNode() {
        return query("queryNode", Node.class, PARAMETER_NAME_OUTPUT_NODE, Neo4jTypes.NTNode);
    }

    public CallableProcedure.BasicProcedure queryRelationship() {
        return query("queryRelationship", Relationship.class, PARAMETER_NAME_OUTPUT_RELATIONSHIP, Neo4jTypes.NTRelationship);
    }

    private <T extends PropertyContainer> CallableProcedure.BasicProcedure query(final String procedureName, final Class<T> clazz, final String outputName, Neo4jTypes.MapType outputType) {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName(procedureName))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in(PARAMETER_NAME_INPUT, Neo4jTypes.NTMap)
                .out(outputName, outputType)
                .out(PARAMETER_NAME_SCORE, Neo4jTypes.NTFloat).build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(Context ctx, Object[] input) throws ProcedureException {
                checkIsMap(input[0]);
                Map<String, Object> inputParams = (Map) input[0];
                String query = (String) inputParams.get(PARAMETER_NAME_QUERY);
                List<SearchMatch<T>> results = searcher.search(query, clazz);
                return Iterators.asRawIterator(getObjectArray(results).iterator());
            }
        };
    }

    public CallableProcedure.BasicProcedure isReindexCompleted() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("initialized"))
                .mode(ProcedureSignature.Mode.READ_ONLY)
                .out(PARAMETER_NAME_STATUS, Neo4jTypes.NTBoolean).build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(Context ctx, Object[] input) throws ProcedureException {
                List<StatusResult> results = new ArrayList<>();
                results.add(new StatusResult(getStartedRuntime(database).getModule(ElasticSearchModule.class).isReindexCompleted()));
                List<Object[]> collector = results.stream().map((r) -> new Object[]{r.status}).collect(Collectors.toList());

                return Iterators.asRawIterator(collector.iterator());
            }
        };
    }

    private <T extends PropertyContainer> List<Object[]> getObjectArray(List<SearchMatch<T>> results) {
        return results.stream()
                .map((result) -> new Object[]{result.getItem(), result.score.floatValue()})
                .collect(Collectors.toList());
    }

    private static void checkIsMap(Object object) throws RuntimeException {
        if (!(object instanceof Map)) {
            throw new RuntimeException("Input parameter is not a map");
        }
    }

    private static ProcedureSignature.ProcedureName getProcedureName(String procedureName) {
        return procedureName("ga", "es", procedureName);
    }

    public void destroy() {
        searcher.destroy();
    }

    class StatusResult {
        public final boolean status;

        public StatusResult(boolean status) {
            this.status = status;
        }
    }
}
