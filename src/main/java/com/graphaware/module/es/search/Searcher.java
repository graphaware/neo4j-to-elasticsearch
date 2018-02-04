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

package com.graphaware.module.es.search;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.graphaware.common.log.LoggerFactory;
import com.graphaware.module.es.ElasticSearchConfiguration;
import com.graphaware.module.es.ElasticSearchModule;
import com.graphaware.module.es.mapping.Mapping;
import com.graphaware.module.es.search.resolver.KeyToIdResolver;
import com.graphaware.module.es.search.resolver.ResolverFactory;
import com.graphaware.module.es.util.CustomJestClientFactory;
import io.searchbox.action.AbstractAction;
import io.searchbox.action.GenericResultAbstractAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.indices.mapping.GetMapping;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.graphaware.runtime.RuntimeRegistry.getStartedRuntime;
import static org.springframework.util.Assert.notNull;

public class Searcher {
    private static final Log LOG = LoggerFactory.getLogger(Searcher.class);

    public final GraphDatabaseService database;
    private final JestClient client;

    private final String keyProperty;
    private final Mapping mapping;
    private final KeyToIdResolver keyResolver;

    public Searcher(GraphDatabaseService database) {
        ElasticSearchConfiguration configuration = (ElasticSearchConfiguration) getStartedRuntime(database).getModule(ElasticSearchModule.class).getConfiguration();

        this.keyProperty = configuration.getKeyProperty();
        this.database = database;
        this.mapping = configuration.getMapping();
        this.keyResolver = ResolverFactory.createResolver(database, mapping.getKeyProperty());
        this.client = createClient(configuration.getProtocol(), configuration.getUri(), configuration.getPort(), configuration.getAuthUser(), configuration.getAuthPassword());
    }

    private Function<SearchMatch, Relationship> getRelationshipResolver() {
        return match -> {
            Relationship rel;
            try {
                long relId = keyResolver.getRelationshipID(match.key);
                rel = database.getRelationshipById(relId);
            } catch (NotFoundException e) {
                rel = null;
            }
            if (rel == null) {
                LOG.warn("Could not find relationship with key (" + keyProperty + "): " + match.key);
            }
            return rel;
        };
    }

    private Function<SearchMatch, Node> getNodeResolver() {
        return match -> {
            Node node = null;
            try {
                long nodeId = keyResolver.getNodeID(match.key);
                node = database.getNodeById(nodeId);
            } catch (NotFoundException e) {
                node = null;
            }
            if (node == null) {
                LOG.warn("Could not find node with key (" + keyProperty + "): " + match.key);
            }
            return node;
        };
    }

    private <T extends Entity> List<SearchMatch<T>> resolveMatchItems(final List<SearchMatch<T>> searchMatches, final Function<SearchMatch, T> resolver) {
        List<SearchMatch<T>> resolvedResults = new ArrayList<>();

        try (Transaction tx = database.beginTx()) {
            searchMatches.stream().forEach(match -> {
                T item = resolver.apply(match);
                if (item != null) {
                    match.setItem(item);
                    resolvedResults.add(match);
                }
            });
            tx.success();
        }
        return resolvedResults;
    }

    private <T extends Entity> List<SearchMatch<T>> buildSearchMatches(SearchResult searchResult) {
        List<SearchMatch<T>> matches = new ArrayList<>();
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

                        // extract the result score
                        JsonElement _score = obj.get("_score");
                        Double score = null;
                        if (_score != null && !_score.isJsonNull() && _score.isJsonPrimitive() && ((JsonPrimitive) _score).isNumber()) {
                            score = _score.getAsDouble();
                        }

                        // extract the result id
                        String keyValue = obj.get("_id") != null ? obj.get("_id").getAsString() : null;
                        if (keyValue == null) {
                            LOG.warn("No key found in search result: " + obj.getAsString());
                        } else {
                            matches.add(new SearchMatch<>(keyValue, score));
                        }
                    }
                });
        return matches;
    }

    public static JestClient createClient(String protocol, String uri, String port, String authUser, String authPassword) {
        notNull(uri);
        notNull(port);

        LOG.info("Creating Jest Client...");

        CustomJestClientFactory factory = new CustomJestClientFactory();
        String esHost = String.format("%s://%s:%s", protocol, uri, port);
        HttpClientConfig.Builder clientConfigBuilder = new HttpClientConfig.Builder(esHost).multiThreaded(true);

        if (authUser != null && authPassword != null) {
            BasicCredentialsProvider customCredentialsProvider = new BasicCredentialsProvider();

            customCredentialsProvider.setCredentials(
                    new AuthScope(uri, Integer.parseInt(port)),
                    new UsernamePasswordCredentials(authUser, authPassword)
            );

            LOG.info("Enabling Auth for ElasticSearch: " + authUser);
            clientConfigBuilder.credentialsProvider(customCredentialsProvider);
        }

        factory.setHttpClientConfig(clientConfigBuilder.build());

        LOG.info("Created Jest Client.");

        return factory.getObject();
    }

    /**
     * @param action The action to send to the index
     * @return the query response
     */
    private <R extends JestResult> R doQuery(AbstractAction<R> action) {
        R result;
        try {
            result = client.execute(action);
        } catch (IOException ex) {
            throw new RuntimeException("Error while performing query on ElasticSearch", ex);
        }
        return result;
    }

    /**
     * @param query the search query to execute
     * @param clazz {@link Node} or {@link Relationship}, to decide which index to send the query to.
     * @param <T>   {@link Node} or {@link Relationship}
     * @return the search results
     */
    private <T extends Entity> SearchResult searchQuery(String query, Class<T> clazz) {
        Search search = new Search.Builder(query).addIndex(mapping.getIndexFor(clazz)).build();
        return doQuery(search);
    }

    /**
     * Search for nodes or relationships
     *
     * @param query An ElasticSearch query in JSON format (serialized as a string)
     * @param clazz {@link Node} or {@link Relationship}
     * @param <T>   {@link Node} or {@link Relationship}
     * @return a list of matches (with node or a relationship)
     */
    public <T extends Entity> List<SearchMatch<T>> search(String query, Class<T> clazz) {
        SearchResult result = searchQuery(query, clazz);

        List<SearchMatch<T>> matches = buildSearchMatches(result);
        @SuppressWarnings("unchecked")
        Function<SearchMatch, T> resolver = (Function<SearchMatch, T>) (
                clazz.equals(Node.class) ? getNodeResolver() : getRelationshipResolver()
        );
        return resolveMatchItems(matches, resolver);
    }

    /**
     * @param query The search query
     * @param clazz The index key ({@link Node} or {@link Relationship})
     * @param <T>   {@link Node} or {@link Relationship}
     * @return a JSON string
     */
    public <T extends Entity> String rawSearch(String query, Class<T> clazz) {
        SearchResult r = searchQuery(query, clazz);
        return r.getJsonString();
    }

    public String nodeMapping() {
        return mappingQuery(Node.class);
    }

    public String relationshipMapping() {
        return mappingQuery(Relationship.class);
    }

    private <T extends Entity> String mappingQuery(Class<T> clazz) {
        String indexName = mapping.getIndexFor(clazz);
        GetMapping getMapping = new GetMapping.Builder().addIndex(indexName).build();
        return doQuery(getMapping)
                .getJsonObject()
                .get(indexName)
                .toString();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (client == null) {
            return;
        }
        client.shutdownClient();
    }

    /***
     * @return the current ElasticSearch nodes information
     */
    public String getEsInfo() {
        return doQuery(new GetVersion.Builder().build()).getJsonString();
    }

    private static class GetVersion extends GenericResultAbstractAction {
        protected GetVersion(Builder builder) {
            super(builder);
            setURI(buildURI());
        }

        @Override
        public String getRestMethodName() {
            return "GET";
        }

        public static class Builder extends AbstractAction.Builder<GetVersion, Builder> {
            @Override
            public GetVersion build() {
                return new GetVersion(this);
            }
        }
    }
}
