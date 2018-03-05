GraphAware Neo4j Elasticsearch Integration (Neo4j Module)
=========================================================

[![Build Status](https://travis-ci.org/graphaware/neo4j-to-elasticsearch.png)](https://travis-ci.org/graphaware/neo4j-to-elasticsearch) | <a href="http://graphaware.com/products/" target="_blank">Downloads</a> | <a href="http://graphaware.com/site/neo4j-to-elasticsearch/latest/apidocs/" target="_blank">Javadoc</a> | Latest Release: 3.3.3.52.7

GraphAware Elasticsearch Integration is an enterprise-grade bi-directional integration between Neo4j and Elasticsearch.
It consists of two independent modules plus a test suite. Both modules can be used independently or together to achieve
full integration.

The first module (this project) is a plugin to Neo4j (more precisely, a [GraphAware Transaction-Driven Runtime Module](https://github.com/graphaware/neo4j-framework/tree/master/runtime#graphaware-runtime)),
which can be configured to transparently and asynchronously replicate data from Neo4j to Elasticsearch. This module is now
production-ready and officially supported by GraphAware for  <a href="http://graphaware.com/enterprise/" target="_blank">GraphAware Enterprise</a> subscribers.

The <a href="https://github.com/graphaware/graph-aided-search" target="_blank">second module (a.k.a. Graph-Aided Search)</a> is a plugin to Elasticsearch that can consult the Neo4j database during an Elasticsearch query to enrich
the result (boost the score) by results that are more efficiently calculated in a graph database, e.g. recommendations.

# Neo4j -> Elasticsearch

## Getting the Software

### Server Mode

When using Neo4j in the <a href="http://docs.neo4j.org/chunked/stable/server-installation.html" target="_blank">standalone server</a> mode,
you will need three (3) .jar files (all of which you can <a href="http://graphaware.com/downloads/" target="_blank">download here</a>)
dropped into the `plugins` directory of your Neo4j installation:

*  <a href="https://github.com/graphaware/neo4j-framework" target="_blank">GraphAware Neo4j Framework</a>
*  <a href="https://github.com/graphaware/neo4j-uuid" target="_blank">GraphAware Neo4j UUID</a> (only required of you are using UUIDs)
*  <a href="https://github.com/graphaware/neo4j-to-elasticsearch" target="_blank">GraphAware Neo4j Elasticsearch Integration</a> (this project)

After changing a few lines of config (read on) and restarting Neo4j, the module will do its magic.

### Embedded Mode / Java Development

Java developers that use Neo4j in <a href="http://docs.neo4j.org/chunked/stable/tutorials-java-embedded.html" target="_blank">embedded mode</a>
and those developing Neo4j <a href="http://docs.neo4j.org/chunked/stable/server-plugins.html" target="_blank">server plugins</a>,
<a href="http://docs.neo4j.org/chunked/stable/server-unmanaged-extensions.html" target="_blank">unmanaged extensions</a>,
GraphAware Runtime Modules, or Spring MVC Controllers can include use the module as a dependency for their Java project.

### Releases

Releases are synced to <a href="http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22uuid%22" target="_blank">Maven Central repository</a>. When using Maven for dependency management, include the following dependency in your pom.xml.

    <dependencies>
        ...
        <dependency>
            <groupId>com.graphaware.integration.es</groupId>
            <!-- this will be com.graphaware.neo4j in the next release -->
            <artifactId>neo4j-to-elasticsearch</artifactId>
            <version>3.3.3.52.7</version>
        </dependency>
        ...
    </dependencies>

#### Snapshots

To use the latest development version, just clone this repository, run `mvn clean install` and change the version in the
dependency above to 3.3.3.52.8-SNAPSHOT.

#### Note on Versioning Scheme

The version number has two parts. The first four numbers indicate compatibility with Neo4j GraphAware Framework.
 The last number is the version of the Elasticsearch Integration library. For example, version 2.3.2.37.1 is version 1 of the Elasticsearch Integration library
 compatible with GraphAware Neo4j Framework 2.3.2.37 (and thus Neo4j 2.3.2).

### Note on UUID

It is a very bad practice to expose internal Neo4j node IDs to external systems. The reason for that is that these IDs
are not guaranteed to be stable and are re-used when nodes are deleted. For this reason, unless you have your own unique
identifier for your nodes already, we highly recommend using <a href="https://github.com/graphaware/neo4j-uuid" target="_blank">GraphAware Neo4j UUID Module</a>
in conjunction with the Elasticsearch Integration Library. The rest of this manual will show you how to do that.

Configuring things as described below means all (or a selected subset of) your nodes will automatically be assigned
an immutable uuid property, which will be indexed in Neo4j and used in Elasticsearch as the key for your indexed nodes
(a.k.a. documents). When Elasticsearch returns a result, it will be the UUID that you will use to retrieve the Node
from Neo4j.

## Setup and Configuration

### Server Mode

Edit `neo4j.conf` to register the required modules:

```

# This setting should only be set once for registering the framework and all the used submodules
dbms.unmanaged_extension_classes=com.graphaware.server=/graphaware

com.graphaware.runtime.enabled=true

#UIDM becomes the module ID:
com.graphaware.module.UIDM.1=com.graphaware.module.uuid.UuidBootstrapper

#optional, default is "uuid". (only if using the UUID module)
com.graphaware.module.UIDM.uuidProperty=uuid

#optional, default is all nodes:
com.graphaware.module.UIDM.node=hasLabel('Label1') || hasLabel('Label2')

#optional, default is uuidIndex
com.graphaware.module.UIDM.uuidIndex=uuidIndex

#prevent the whole db to be assigned a new uuid if the uuid module is settle up together with neo4j2es
com.graphaware.module.UIDM.initializeUntil=0

#ES becomes the module ID:
com.graphaware.module.ES.2=com.graphaware.module.es.ElasticSearchModuleBootstrapper

#URI of Elasticsearch
com.graphaware.module.ES.uri=localhost

#Port of Elasticsearch
com.graphaware.module.ES.port=9201

#optional, protocol of Elasticsearch connection, defaults to http
com.graphaware.module.ES.protocol=http

#optional, Elasticsearch index name, default is neo4j-index
com.graphaware.module.ES.index=neo4j-index

#optional, node property key of a propery that is used as unique identifier of the node. Must be the same as com.graphaware.module.UIDM.uuidProperty (only if using UUID module), defaults to uuid
#use "ID()" to use native Neo4j IDs as Elasticsearch IDs (not recommended)
com.graphaware.module.ES.keyProperty=uuid

#optional, whether to retry if a replication fails, defaults to false
com.graphaware.module.ES.retryOnError=false

#optional, size of the in-memory queue that queues up operations to be synchronised to Elasticsearch, defaults to 10000
com.graphaware.module.ES.queueSize=10000

#optional, size of the batch size to use during re-initialization, defaults to 1000
com.graphaware.module.ES.reindexBatchSize=2000

#optional, specify which nodes to index in Elasticsearch, defaults to all nodes
com.graphaware.module.ES.node=hasLabel('Person')

#optional, specify which node properties to index in Elasticsearch, defaults to all properties
com.graphaware.module.ES.node.property=key != 'age'

#optional, specify whether to send updates to Elasticsearch in bulk, defaults to true (highly recommended)
com.graphaware.module.ES.bulk=true

#optional, read explanation below, defaults to 0
com.graphaware.module.ES.initializeUntil=0

#optional, whether or not the reindexation process (when db start) should be made in asynchronous mode
#default is "false" and the db will not be available until completed
#com.graphaware.module.ES.asyncIndexation=true

```

For explanation of the UUID configurations, please see the [UUID Module docs](https://github.com/graphaware/neo4j-uuid).

For explanation of the syntax used in the configuration, refer to the [Inclusion Policies](https://github.com/graphaware/neo4j-framework/tree/master/common#inclusion-policies).

The Elasticsearch Integration configuration is described in the inline comments above. The only property that needs a little
more explanation is `com.graphaware.module.ES.initializeUntil`:

Every GraphAware Framework Module has methods (`initialize()` and `reinitialize()`) that provide a mechanism to get the
world into a state equivalent to a situation in which the module has been running since the database was empty.
These methods kick in in one of the following scenarios:

* The database is not empty when the module has been registered for the first time (GraphAware Framework used on an existing database)
* The configuration of the module has changed since the last time it was run
* Some failure occurred that causes the Framework to think it should fix things.

We've decided that we should not shoot the whole database at Elasticsearch in one of these scenarios automatically, because
it could well be quite large. Therefore, in order to trigger (re-)indexing, i.e. sending every node that should be indexed
to Elasticsearch upon Neo4j restart, you have to manually intervene.

The way you intervene is set the com.graphaware.module.ES.initializeUntil to a number slightly higher than a Java call to `System.currentTimeInMillis()`
would return when the module is starting. This way, the database will be (re-)indexed once, not with every following restart.
In other words, re-indexing will happen iff `System.currentTimeInMillis() < com.graphaware.module.ES.initializeUntil`.
If you're not sure what all of this means or don't know how to find the right number to set this value to, you're probably
best off leaving it alone or getting in touch for some (paid) support.


#### ElasticSearch Shield Support

If Shield plugin is installed and enabled on Elasticsearch node, it is possible to add authentication parameters in the configuration.
Here an example:

```
#optional, specify the Shield user
com.graphaware.module.ES.authUser=neo4j_user

#optional, specify the Shield password
com.graphaware.module.ES.authPassword=123456
```

Both of them MUST be specified to enabling Authentication. The user must be able to perform writes on the elasticsearch instance.

### Embedded Mode / Java Development

To use the ElasticSearch Integration Module programmatically, register the module like this

```java
GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database); //where database is an instance of GraphDatabaseService
runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration(), database));

configuration = ElasticSearchConfiguration.defaultConfiguration(HOST, PORT);
runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

runtime.start();
```

Alternatively:
```java
 GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(pathToDb)
    .loadPropertiesFromFile(this.getClass().getClassLoader().getResource("neo4j.properties").getPath())
    .newGraphDatabase();

 //make sure neo4j.properties contain the lines mentioned in previous section
```

## Usage

Apart from the configuration described above, the GraphAware ElasticSearch Integration Module requires nothing else to function.
It will replicate transactions asynchronously to ElasticSearch.

### Cypher Procedures

This module provides a set of Cypher procedures that allows communicate with Elasticsearch using the Cypher query language.
These are the available procedures:

#### Searching for nodes or relationships

This procedures allows to perform search queries on indexed nodes or relationships and return them for further use in the cypher query.
Example of usage:

```
CALL ga.es.queryNode('{\"query\":{\"match\":{\"name\":\"alessandro\"}}}') YIELD node, score RETURN node, score"
```

Together with the nodes also the related score is returned.

Any search query can be submitted through the procedure, it will be performed on the index configured
for replication on Elasticsearch.

Similar procedures are `queryNodeRaw` and `queryRelationshipRaw` procedures. These procedures are similar to the `queryNode` and `queryRelationship` (they accept the same parameters) but they return a JSON-encoded value of the node or relationship as returned by Ealsticsearch.
Example:
```
CALL ga.es.queryRelationshipRaw('{\"query\":{\"match\":{\"city\":\"paris\"}}}') YIELD json, score RETURN json, score"
```

#### Monitoring the status of the reindexing process

Depending on your configuration, the module can be in `initialization` mode when starting, processing a complete reindexing
of the Neo4j graph database content (in accordance with your configuration settings)

You can monitor the status of the `init` mode:
```
CALL ga.es.initialized() YIELD status RETURN status
```

Returns `true` or `false`

#### Getting the current node or relationship mapping

You can retrieve the current node or relationship mapping from Elasticsearch using the following procedure:

```
CALL ga.es.nodeMapping() YIELD json as mapping RETURN mapping
```
or
```
CALL ga.es.relationshipMapping() YIELD json as mapping RETURN mapping
```

This will return a JSON string containing the mapping returned by Elasticsearch's [Get Mapping API](https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-mapping.html).
The returned JSON string needs to be decoded using JSON parsing library.

#### Getting Elasticsearch information

```
CALL ga.es.info() YIELD json as info return info
```

This will return a JSON string containing Elasticsearch server information as returned bas the Basic Status API.
The returned JSON string needs to be decoded using JSON parsing library.
An example of parsed result:
```
{
  "name" : "Sharon Friedlander",
  "cluster_name" : "elasticsearch",
  "version" : {
    "number" : "2.4.0",
    "build_hash" : "ce9f0c7394dee074091dd1bc4e9469251181fc55",
    "build_timestamp" : "2016-08-29T09:14:17Z",
    "build_snapshot" : false,
    "lucene_version" : "5.5.2"
  },
  "tagline" : "You Know, for Search"
}
```

### Version of ElasticSearch

This module has been tested with ElasticSearch 2.3.0+.

License
-------

Copyright (c) 2015-2017 GraphAware

GraphAware is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License
as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with this program.
If not, see <http://www.gnu.org/licenses/>.
