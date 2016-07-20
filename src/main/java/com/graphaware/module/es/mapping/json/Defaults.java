package com.graphaware.module.es.mapping.json;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Defaults {

    private static final boolean DEFAULT_INCLUDE_REMAINING = true;

    @JsonProperty("key_property")
    private String keyProperty;

    private String index;

    @JsonProperty("node_index")
    private String nodeIndex;

    @JsonProperty("relationship_index")
    private String relationshipIndex;

    @JsonProperty("include_remaining_properties")
    private Boolean includeRemainingProperties;

    @JsonProperty("blacklisted_node_properties")
    private List<String> blacklistedNodeProperties;

    public String getKeyProperty() {
        return keyProperty;
    }
    public String getIndex() {
        return index;
    }

    public String getNodeIndex() {
        return nodeIndex;
    }

    public String getRelationshipIndex() {
        return relationshipIndex;
    }

    public boolean includeRemainingProperties() {
        return includeRemainingProperties != null ? includeRemainingProperties : DEFAULT_INCLUDE_REMAINING;
    }

    public List<String> getBlacklistedNodeProperties() {
        return blacklistedNodeProperties;
    }
}
