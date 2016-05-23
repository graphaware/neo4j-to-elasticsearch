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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.graphaware.module.es.ElasticSearchConfiguration;
import com.graphaware.module.es.ElasticSearchModule;
import static com.graphaware.runtime.RuntimeRegistry.getStartedRuntime;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;
import org.slf4j.LoggerFactory;

public class ElasticSearchProcedure {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ElasticSearchProcedure.class);

    private static final String PARAMETER_NAME_INPUT = "input";
    private static final String PARAMETER_NAME_QUERY = "query";
    private static final String PARAMETER_NAME_OUTPUT = "node";
    private final GraphDatabaseService database;
    private final String uri;
    private final String port;
    private final String keyProperty;
    private final String index;
    private final String authUser;
    private final String authPassword;

    private final ElasticSearchConfiguration configuration;
    private final JestClient client;

    public ElasticSearchProcedure(GraphDatabaseService database) {
        this.database = database;
        configuration = (ElasticSearchConfiguration) getStartedRuntime(database).getModule(ElasticSearchModule.class).getConfiguration();
        this.uri = configuration.getUri();
        this.port = configuration.getPort();
        this.keyProperty = configuration.getKeyProperty();
        this.index = configuration.getIndex();
        this.authUser = configuration.getAuthUser();
        this.authPassword = configuration.getAuthPassword();
        this.client = createClient();
    }

    public CallableProcedure.BasicProcedure query() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("query"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in(PARAMETER_NAME_INPUT, Neo4jTypes.NTMap)
                .out(PARAMETER_NAME_OUTPUT, Neo4jTypes.NTNode).build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(Context ctx, Object[] input) throws ProcedureException {
                checkIsMap(input[0]);
                Map<String, Object> inputParams = (Map) input[0];
                String query = (String) inputParams.get(PARAMETER_NAME_QUERY);
                List<Node> nodes = performQuery(query);
                return Iterators.asRawIterator(getObjectArray(nodes).iterator());
            }

        };
    }

    private List<Node> performQuery(String query) {
        Search search = new Search.Builder(query)
                .addIndex(index)
                .build();

        SearchResult result;
        try {
            result = client.execute(search);
        } catch (IOException ex) {
            throw new RuntimeException("Error while performing query on es", ex);
        }
        List<KeySearchResult> keys = extractKeys(result);
        List<Node> nodes = extractNodesFromKeys(keys);
        return nodes;

    }

    private List<Object[]> getObjectArray(List<Node> nodes) {
        List<Object[]> collector = nodes.stream()
                .map((node) -> new Object[]{node})
                .collect(Collectors.toList());
        return collector;
    }

    protected final JestClient createClient() {
        LOG.info("Creating Jest Client...");

        JestClientFactory factory = new JestClientFactory();
        String esHost = String.format("http://%s:%s", uri, port);
        HttpClientConfig.Builder clientConfigBuilder
                = new HttpClientConfig.Builder(esHost).multiThreaded(true);
        if (authUser != null && authPassword != null) {
            BasicCredentialsProvider customCredentialsProvider = new BasicCredentialsProvider();
            customCredentialsProvider.setCredentials(
                    new AuthScope(uri, Integer.parseInt(port)),
                    new UsernamePasswordCredentials(authUser, authPassword));
            LOG.info("Enabling Auth for elasticsearch: " + authUser);
            clientConfigBuilder.credentialsProvider(customCredentialsProvider);
        }
        factory.setHttpClientConfig(clientConfigBuilder
                .build());

        LOG.info("Created Jest Client.");

        return factory.getObject();
    }

    protected void checkIsMap(Object object) throws RuntimeException {
        if (!(object instanceof Map)) {
            throw new RuntimeException("Input parameter is not a map");
        }
    }

    protected static ProcedureSignature.ProcedureName getProcedureName(String procedureName) {
        return procedureName("ga", "es", procedureName);
    }

    private List<KeySearchResult> extractKeys(SearchResult searchResult) {
        List<KeySearchResult> result = new ArrayList<>();
        Set<Map.Entry<String, JsonElement>> entrySet = searchResult.getJsonObject().entrySet();
        entrySet.stream()
                .filter((item) -> (item.getKey().equalsIgnoreCase("hits")))
                .map((item) -> (JsonObject) item.getValue())
                .filter((hits) -> (hits != null))
                .map((hits) -> hits.getAsJsonArray("hits"))
                .filter((hitsArray) -> (hitsArray != null))
                .forEach((hitsArray) -> {
                    for (JsonElement element : hitsArray) {
                        JsonObject obj = (JsonObject) element;
                        String type = obj.get("_type").getAsString();
                        if (obj.get("_source") == null) {
                            throw new RuntimeException("No _source in the elasticsearch response");
                        }
                        JsonObject source = obj.get("_source").getAsJsonObject();
                        String keyField = source.get(keyProperty) != null ? source.get(keyProperty).getAsString() : null;
                        if (keyField == null) {
                            LOG.warn("No keyProperty " + keyProperty + " found in document: " + source);
                        } else {
                            result.add(new KeySearchResult(type, keyField));
                        }
                    }
                });
        return result;
    }

    private List<Node> extractNodesFromKeys(List<KeySearchResult> keys) {
        List<Node> result = new ArrayList<>();
        try (Transaction tx = database.beginTx()) {
            keys.stream().forEach((key) -> {
                Node node = database.findNode(Label.label(key.getType()), keyProperty, key.getKeyProperty());
                if (node != null) {
                    result.add(node);
                } else {
                    LOG.warn("Not found node with label " + key.getType() + " and keyProperty " + key.getKeyProperty());
                }
            });
            tx.success();
        }
        return result;
    }

    class KeySearchResult {

        private String type;
        private String keyProperty;

        public KeySearchResult(String type, String keyProperty) {
            this.type = type;
            this.keyProperty = keyProperty;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getKeyProperty() {
            return keyProperty;
        }

        public void setKeyProperty(String keyProperty) {
            this.keyProperty = keyProperty;
        }

    }
}
