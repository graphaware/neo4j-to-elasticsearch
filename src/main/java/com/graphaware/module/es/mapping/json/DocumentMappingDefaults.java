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

package com.graphaware.module.es.mapping.json;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class DocumentMappingDefaults {

    private static final boolean DEFAULT_INCLUDE_REMAINING = true;
    private static final List<String> DEFAULT_BLACKLIST = new ArrayList<>();
    private static final boolean DEFAULT_SKIP_NULL_PROPERTIES = false;

    @JsonProperty("key_property")
    private String keyProperty;

    @JsonProperty("nodes_index")
    private String defaultNodesIndex;

    @JsonProperty("relationships_index")
    private String defaultRelationshipsIndex;

    @JsonProperty("include_remaining_properties")
    private Boolean includeRemainingProperties;

    @JsonProperty("blacklisted_node_properties")
    private List<String> blacklistedNodeProperties;

    @JsonProperty("blacklisted_relationship_properties")
    private List<String> blacklistedRelationshipProperties;

    @JsonProperty("exclude_empty_properties")
    private Boolean excludeEmptyProperties;

    public String getKeyProperty() {
        return keyProperty;
    }

    public String getDefaultNodesIndex() {
        return defaultNodesIndex;
    }

    public String getDefaultRelationshipsIndex() {
        return defaultRelationshipsIndex;
    }

    public boolean includeRemainingProperties() {
        return includeRemainingProperties != null ? includeRemainingProperties : DEFAULT_INCLUDE_REMAINING;
    }

    public List<String> getBlacklistedNodeProperties() {
        return null != blacklistedNodeProperties ? blacklistedNodeProperties : DEFAULT_BLACKLIST;
    }

    public List<String> getBlacklistedRelationshipProperties() {
        return null != blacklistedRelationshipProperties ? blacklistedRelationshipProperties : DEFAULT_BLACKLIST;
    }

    public boolean excludeEmptyProperties() {
        return null != excludeEmptyProperties ? excludeEmptyProperties : DEFAULT_SKIP_NULL_PROPERTIES;
    }
}
