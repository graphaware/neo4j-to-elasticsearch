package com.graphaware.module.es.mapping.json;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class DocumentMappingDefaults {

    private static final boolean DEFAULT_INCLUDE_REMAINING = true;
    private static final List<String> DEFAULT_BLACKLIST = new ArrayList<>();

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
    private boolean excludeEmptyProperties;

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
        return excludeEmptyProperties;
    }
}
