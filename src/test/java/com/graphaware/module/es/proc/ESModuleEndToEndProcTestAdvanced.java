/*
 * Copyright (c) 2013-2016 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.es.proc;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.graphaware.module.es.mapping.AdvancedMapping;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests ElasticSearch procedures with a configuration htat uses:
 * - mapping=AdvancedMapping
 * - propertyKey=uuid (uses UuidResolver)
 */
public class ESModuleEndToEndProcTestAdvanced extends ESProcedureIntegrationTest {

    @Override
    protected String configFile() {
        return "integration/int-test-advancedMapping-uuidKeys.conf";
    }

    public void testEsMapping(boolean node) {
        String itemType = node ? "node" : "relationship";

        // match all items
        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.es." + itemType + "Mapping() YIELD json return json");

            List<String> columns = result.columns();
            assertEquals(columns.size(), 1);

            Map<String, Object> next = result.next();
            assertTrue(next.get("json") instanceof String);
            JsonObject e = new JsonParser().parse((String) next.get("json")).getAsJsonObject();

            assertTrue(e.has("mappings"));
            assertTrue(e.get("mappings").isJsonObject());

            // item types
            e.get("mappings").getAsJsonObject().entrySet().stream().forEach(typeEntry -> {
                // item type
                assertTrue(typeEntry.getKey() instanceof String);
                assertEquals(typeEntry.getKey(), itemType);

                // item properties
                assertTrue(typeEntry.getValue() instanceof JsonObject);
                JsonObject typeProps = (JsonObject) typeEntry.getValue();
                assertTrue(typeProps.has("properties"));

                JsonObject propertiesMapping = typeProps.get("properties").getAsJsonObject();
                propertiesMapping.entrySet().stream().forEach(propertyMapping -> {
                    // property key
                    assertTrue(propertyMapping.getKey() instanceof String);

                    // property mapping info
                    assertTrue(propertyMapping.getValue() instanceof JsonObject);
                    JsonObject propertyInfo = (JsonObject) propertyMapping.getValue();
                    assertTrue(propertyInfo.has("type"));
                });

                String categField = node
                        ? AdvancedMapping.LABELS_FIELD
                        : AdvancedMapping.RELATIONSHIP_FIELD;

                assertTrue(propertiesMapping.has(categField));
                assertTrue(propertiesMapping.get(categField).isJsonObject());
                JsonObject categMapping = propertiesMapping.get(categField).getAsJsonObject();
                // categ field has type:string
                categMapping.get("type").isJsonPrimitive();
                assertEquals(categMapping.get("type").getAsString(), "string");
                // categ field has fields:{raw:{type:string, index:not_analyzed}}
                assertTrue(categMapping.get("fields").isJsonObject());
                assertTrue(categMapping.get("fields").getAsJsonObject().has("raw"));
                assertTrue(categMapping.get("fields").getAsJsonObject().get("raw").isJsonObject());
                JsonObject rawField = categMapping.get("fields").getAsJsonObject().get("raw").getAsJsonObject();
                // raw categ field
                assertTrue(rawField.has("type"));
                assertTrue(rawField.get("type").isJsonPrimitive());
                assertEquals(rawField.get("type").getAsString(), "string");
                assertTrue(rawField.has("index"));
                assertTrue(rawField.get("index").isJsonPrimitive());
                assertEquals(rawField.get("index").getAsString(), "not_analyzed");
            });

            tx.success();
        }
    }
}
