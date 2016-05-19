GraphAware Neo4j Elasticsearch Integration (Neo4j Module)
=========================================================

[![Build Status](https://travis-ci.org/graphaware/neo4j-to-elasticsearch.png)](https://travis-ci.org/graphaware/neo4j-to-elasticsearch) | <a href="http://graphaware.com/products/" target="_blank">Downloads</a> | <a href="http://graphaware.com/site/neo4j-to-elasticsearch/latest/apidocs/" target="_blank">Javadoc</a> | Latest Release: 3.0.1.38.1

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
*  <a href="https://github.com/graphaware/neo4j-uuid" target="_blank">GraphAware Neo4j UUID</a>
*  <a href="https://github.com/graphaware/neo4j-to-elasticsearch" target="_blank">GraphAware Neo4j Elasticsearch Integration</a> (this project)

After changing a few lines of config (read on) and restarting Neo4j, the module will do its magic.

### Embedded Mode / Java Development

Java developers that use Neo4j in <a href="http://docs.neo4j.org/chunked/stable/tutorials-java-embedded.html" target="_blank">embedded mode</a>
and those developing Neo4j <a href="http://docs.neo4j.org/chunked/stable/server-plugins.html" target="_blank">server plugins</a>,
<a href="http://docs.neo4j.org/chunked/stable/server-unmanaged-extensions.html" target="_blank">unmanaged extensions</a>,
GraphAware Runtime Modules, or Spring MVC Controllers can include use the module as a dependency for their Java project.

#### Releases

Releases are synced to <a href="http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22uuid%22" target="_blank">Maven Central repository</a>. When using Maven for dependency management, include the following dependency in your pom.xml.

    <dependencies>
        ...
        <dependency>
            <groupId>com.graphaware.integration.es</groupId>
            <!-- this will be com.graphaware.neo4j in the next release -->
            <artifactId>neo4j-to-elasticsearch</artifactId>
            <version>3.0.1.38.1</version>
        </dependency>
        ...
    </dependencies>

#### Snapshots

To use the latest development version, just clone this repository, run `mvn clean install` and change the version in the
dependency above to 3.0.1.38.2-SNAPSHOT.

#### Note on Versioning Scheme

The version number has two parts. The first four numbers indicate compatibility with Neo4j GraphAware Framework.
 The last number is the version of the Elasticsearch Integration library. For example, version 2.3.2.37.1 is version 1 of the Elasticsearch Integration library
 compatible with GraphAware Neo4j Framework 2.3.2.37 (and thus Neo4j 2.3.2).

#### Note on UUID

It is a very bad practice to expose internal Neo4j node IDs to external systems. The reason for that is that these IDs
are not guaranteed to be stable and are re-used when nodes are deleted. For this reason, unless you have your own unique
identifier for your nodes already, we highly recommend using <a href="https://github.com/graphaware/neo4j-uuid" target="_blank">GraphAware Neo4j UUID Module</a>
in conjunction with the Elasticsearch Integration Library. The rest of this manual will show you how to do that.

Configuring things as described below means all (or a selected subset of) your nodes will automatically be assigned
an immutable uuid property, which will be indexed in Neo4j and used in Elasticsearch as the key for your indexed nodes
(a.k.a. documents). When Elasticsearch returns a result, it will be the UUID that you will use to retrieve the Node
from Neo4j.

#### Setup and Configuration

##### Server Mode

Edit `neo4j.properties` to register the required modules:

```
com.graphaware.runtime.enabled=true

#UIDM becomes the module ID:
com.graphaware.module.UIDM.1=com.graphaware.module.uuid.UuidBootstrapper

#optional, default is uuid:
com.graphaware.module.UIDM.uuidProperty=uuid

#optional, default is all nodes:
com.graphaware.module.UIDM.node=hasLabel('Label1') || hasLabel('Label2')

#optional, default is uuidIndex
com.graphaware.module.UIDM.uuidIndex=uuidIndex

#ES becomes the module ID:
com.graphaware.module.ES.2=com.graphaware.module.es.ElasticSearchModuleBootstrapper

#URI of Elasticsearch
com.graphaware.module.ES.uri=localhost

#Port of Elasticsearch
com.graphaware.module.ES.port=9201

#optional, Elasticsearch index name, default is neo4j-index
com.graphaware.module.ES.index=neo4j-index

#optional, node property key of a propery that is used as unique identifier of the node. Must be the same as com.graphaware.module.UIDM.uuidProperty, defaults to uuid
com.graphaware.module.ES.keyProperty=uuid

#optional, whether to retry if a replication fails, defaults to false
com.graphaware.module.ES.retryOnError=false

#optional, size of the in-memory queue that queues up operations to be synchronised to Elasticsearch, defaults to 10000
com.graphaware.module.ES.queueSize=10000

#optional, specify which nodes to index in Elasticsearch, defaults to all nodes
com.graphaware.module.ES.node=hasLabel('Person')

#optional, specify which node properties to index in Elasticsearch, defaults to all properties
com.graphaware.module.ES.node.property=key != 'age'

#optional, specify whether to send updates to Elasticsearch in bulk, defaults to true (highly recommended)
com.graphaware.module.ES.bulk=true

#optional, read explanation below, defaults to 0
com.graphaware.module.ES.initializeUntil=0

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


##### ElasticSearch Shield Support

If Shield plugin is installed and enabled on Elasticsearch node, it is possible to add authentication parameters in the configuration.
Here an example:

#optional, specify the Shield user
com.graphaware.module.ES.authUser=neo4j_user

#optional, specify the Shield password
com.graphaware.module.ES.authPassword=123456

Both of them MUST be specified to enabling Authentication. The user must be able to perform writes on the elasticsearch instance.

##### Embedded Mode / Java Development

To use the Elasticsearch Integration Module programmatically, register the module like this

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

#### Usage

Apart from the configuration described above, the GraphAware Elasticsearch Integration Module requires nothing else to function.
It will replicate transactions asynchronously to Elasticsearch.

#### Version of Elasticsearch

This module has been tested with Elasticsearch 2.3.0+.

License
-------

Copyright (c) 2015 GraphAware

GraphAware is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License
as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with this program.
If not, see <http://www.gnu.org/licenses/>.
