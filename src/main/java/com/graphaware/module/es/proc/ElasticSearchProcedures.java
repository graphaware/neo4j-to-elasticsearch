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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.neo4j.kernel.api.exceptions.KernelException;

@Component
public class ElasticSearchProcedures {

    private final GraphDatabaseService database;
    private final Procedures procedures;
    private ElasticSearchProcedure esProcedures;

    @Autowired
    public ElasticSearchProcedures(GraphDatabaseService database, Procedures procedures) {
        this.database = database;
        this.procedures = procedures;
    }

    @PostConstruct
    public void init() throws ProcedureException, KernelException {
        esProcedures = new ElasticSearchProcedure(database);
        procedures.register(esProcedures.queryNode());
        procedures.register(esProcedures.queryRelationship());
        procedures.register(esProcedures.isReindexCompleted());
    }
    
    @PreDestroy
    public void destroy() {
        if (esProcedures == null) { return; }
        esProcedures.destroy();
    }
}
