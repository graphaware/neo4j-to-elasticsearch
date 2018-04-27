## Json File Mapping Definition

The `JsonFileMapper` is the most flexible way to define your mapping definition for the replication to Elasticsearch.

The main features are :

* default and per node labels / relationship types index definitions
* combination of node / relationship properties into one elasticsearch document field
* dynamic indices (for example time-based indices like `logs-2016-07-18`)
* expressive definition thanks to Spring Expression Language

### Configuration

Configure the UUID and the ES modules like for the basic configurations

```
com.graphaware.runtime.enabled=true
com.graphaware.module.UIDM.1=com.graphaware.module.uuid.UuidBootstrapper
com.graphaware.module.UIDM.uuidProperty=id
com.graphaware.module.UIDM.relationship=(true)

com.graphaware.module.ES.2=com.graphaware.module.es.ElasticSearchModuleBootstrapper
com.graphaware.module.ES.uri=localhost
com.graphaware.module.ES.port=9201
com.graphaware.module.ES.keyProperty=id
com.graphaware.module.ES.retryOnError=false
com.graphaware.module.ES.queueSize=1000
com.graphaware.module.ES.node=
com.graphaware.module.ES.relationship=(true)
com.graphaware.module.ES.bulk=true
com.graphaware.module.ES.initializeUntil=0
com.graphaware.module.ES.mapping=com.graphaware.module.es.mapping.JsonFileMapping
com.graphaware.module.ES.file=mapping.json
```

The last two lines in the configuration specify the type of mapper to use and the name of the file where
the json mapping will be defined (this file **must** reside in the `conf` directory of your neo4j instance)

### Mapping definition

Let's start with a default json mapping definition

```json
{
  "defaults": {
    "key_property": "uuid",
    "nodes_index": "default-index-node",
    "relationships_index": "default-index-relationship",
    "include_remaining_properties": true
  },
  "node_mappings": [
    {
      "condition": "hasLabel('Person')",
      "type": "persons",
      "properties": {
        "name": "getProperty('firstName') + ' ' + getProperty('lastName')"
      }
    }
  ],
  "relationship_mappings": [
    {
      "condition": "isType('WORKS_FOR')",
      "type": "workers"
    }
  ]
}
```

There is basically three sections :

* `defaults` : which configure some defaults replication/mapping settings
* `node_mappings` : where you configure one or multiple definitions for nodes
* `relationship_mappings` : where you configure one or multiple definitions for relationships

The `defaults` configuration can contain the following :

* `key_property` : defines the key representing the `id` that will be used for elasticsearch documents
* `nodes_index` : the default elasticsearch index where nodes will be indexed
* `relationships_index` : the default elasticsearch index where relationships will be indexed
* `include_remaining_properties` : if node/relationship properties not explicitly defined in the mapping should be included in the documents

### Mappings

This simple example has two definitions, one for nodes and one for relationships. Let's analyze the first one :


```json
{
      "condition": "hasLabel('Person')",
      "type": "persons",
      "properties": {
        "name": "getProperty('firstName') + ' ' + getProperty('lastName')"
      }
}
```

The condition will check this mapping is valid for the current node to be indexed, it has a `condition` checking if the node
contains the `Person` label.

The `type` defines the type to use for indexing the elasticsearch document.

The `properties` has one definition for `name` representing an elasticsearch document field. It uses the expression language
to combine two node properties together.

As it doesn't have an explicit index, the index from the `defaults` will be used, in that case `default-index-node`.

You can explicitly define the index for this particular type of node by adding an index in the node mapping :

```json
{
      "condition": "hasLabel('Person')",
      "index": "nodes-persons",
      "type": "persons",
      "properties": {
        "name": "getProperty('firstName') + ' ' + getProperty('lastName')"
      }
}
```

You can combine expressions for the `condition`, example :

```json
{
      "condition": "hasLabel('Person') && !hasLabel('BannedUser') && getProperty('registration_confirmed') == true",
      "index": "nodes-persons",
      "type": "persons",
      "properties": {
        "name": "getProperty('firstName') + ' ' + getProperty('lastName')"
      }
}
```

The same applies for relationship mappings.

### Indexing labels of a node

You can create a field definition containing an array of strings representing the labels, for example :

```json
{
      "condition": "hasLabel('Person')",
      "index": "nodes-persons",
      "type": "persons",
      "properties": {
        "name": "getProperty('firstName') + ' ' + getProperty('lastName')",
        "internal_neo4j_labels": "getLabels()"
      }
}
```

### Dealing with nodes without labels

For indexing nodes without labels, you verify the length of the labels array in the condition :

```json
{
      "condition": "getLabels().length == 0",
      "index": "nodes-default-index",
      "type": "nodes-unknown"
}
```

### Indexing all nodes and relationships :

There are two expression helpers for this kind of usage, respectively `allNodes()` and `allRelationships()` :

```json
{
  "defaults": {
    "key_property": "uuid",
    "nodes_index": "default-index-node",
    "relationships_index": "default-index-relationship",
    "include_remaining_properties": true
  },
  "node_mappings": [
    {
      "condition": "allNodes()",
      "type": "nodes"
    }
  ],
  "relationship_mappings": [
    {
      "condition": "allRelationships()",
      "type": "relationships"
    }
  ]
}
```

### Dynamic index or type names

You can use expressions also for defining the name of the index or the type, for example if you want to have index names based
on the value of a property, you can use the following as example :

```json
{
      "condition": "getLabels().length > 0",
      "index": "'nodes-' + getProperty('actionType').toLower()'",
      "type": "action"
}
```

Or for time based indices, a helper method can be used for converting a node/relationship property timestamp to a formatted
date string :

```json
{
      "condition": "hasLabel('Tweet')",
      "index": "'tweets-' + formatTime('timestamp','YYYY-MM-dd')",
      "type": "tweets"
}
```

Note that the time **must** be in milliseconds. `'timestamp'` argument represent the property key on the node or relationship.

A third argument can be used for defining the timezone :

```json
{
      "condition": "hasLabel('Tweet')",
      "index": "'tweets-' + formatTime('timestamp','YYYY-MM-dd', 'GMT+10')",
      "type": "tweets"
}
```

### Blacklisting properties

Sometimes you may want to not index some properties of nodes or relationships, like `password` or any other sensitive data.

You can predefine a list of blacklisted node / relationship property keys that shouldn't be indexed, example :

```json
{
  "defaults": {
    "key_property": "uuid",
    "nodes_index": "node-index",
    "relationships_index": "relationship-index",
    "include_remaining_properties": true,
    "blacklisted_node_properties": ["password"],
    "blacklisted_relationship_properties": ["uuid"],
    "exclude_empty_properties": false
  },
  "node_mappings": [
    {
      "condition": "hasLabel('Person')",
      "type": "persons",
      "properties": {
        "name": "getProperty('firstName') + ' ' + getProperty('lastName')",
        "labels": "getLabels()"
      }
    }
  ]
}
```

With the above configuration, passwords will never be included in the json source of the ES documents neither the
relationship uuid properties.

### Using Cypher queries for replication logic :

It is possible to trigger a Cypher query execution as logic for returning a value to be set as ES document field,
for example :

```json
{
  "defaults": {
    "key_property": "uuid",
    "include_remaining_properties": true,
    "blacklisted_node_properties": ["password", "uuid"]
  },
  "node_mappings": [
    {
      "condition": "hasLabel('Document') && hasLabel('ReplicationReady')",
      "type": "documents",
      "index": "documents",
      "properties": {
        "title": "getProperty('title')",
        "text": "getProperty('text')",
        "keywords": "query('MATCH (n) WHERE id(n) = {id} MATCH (n)-[:HAS_ANNOTATED_TEXT]->(at)<-[r:DESCRIBES]-(k) apoc.coll.sortMaps(collect({keyword: k.value, relevance: r.relevance}), \"relevance\")[0..5] AS value')"
      }
    }
  ],
  "relationship_mappings": []
}
```

The above mapping defines that the `keywords` field of the ES document should be filled with the result of the query.

**Note:** The result of the query **MUST** have the `value` identifier and **MUST** return a datatype compatible with Elasticsearch.

### Hot reload of the mapping configuration

During development, it is very useful to be able to test the mapping configuration. You can now reload it after any changes to your `mapping.json` file with the
following procedure :

```
CALL ga.es.reloadMapping()
```

