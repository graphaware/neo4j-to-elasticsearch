package com.graphaware.module.es.mapping.json;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class DocumentMappingDefaults {

    private static final boolean DEFAULT_INCLUDE_REMAINING = true;

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
        return blacklistedNodeProperties;
    }
}
