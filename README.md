rocketmq-schema-registry
================

RocketMQ schema registry is a management platform for Avro schema of RocketMQ Topic, which provides a restful interface
for store, delete, update and query schema. Schema register will generate new schema version in every update request.
Therefore, during schema evolution, the platform supports formatting and verification based on specified compatibility
configurations. By default, seven compatibility policies are supported. Schemas can evolve based on a unique subject,
and each Schema version in the evolution can be individually referenced to other subjects. By binding subject to the
Schema, the New RocketMQ client can send data based on a user-specified structure without requiring the user to care
about the details of serialization and deserialization

It offers a variety of features:

* Handle basic schema management operation including store, query, update, delete
* Encoding / Decoding capacity by user specified serializer / deserializer in client
* Compatibility validate in duration of schema evolution or send/receive message
* Create reference between schema version and a new subject
* Currently, only the Avro type is supported. Json, PB, and Thrift types will be extended later

Getting started
--------------

#### Installation

```shell
git clone git@github.com:apache/rocketmq-schema-registry.git
cd rocketmq-schema-registry
./mvn clean package
```

#### Prepare storage layer

Currently, only RocketMQ is supported as the storage layer. And relies on the Compact Topic feature of RocketMQ 5.0,
although previous versions can also be worked, but there is a risk of data loss if the machine disk fails. Similarly,
DB-type storage layers will be extended in the future.

On rocketmq storage type, we need to start a RocketMQ namesrv and broker service first.

```shell
# Download release from the Apache mirror
$ wget https://archive.apache.org/dist/rocketmq/4.9.3/rocketmq-all-4.9.3-bin-release.zip

# Unpack the release
$ unzip rocketmq-all-4.9.3-bin-release.zip

# Prepare a terminal and change to the extracted `bin` directory:
$ cd rocketmq-4.9.3/bin

# Start namesrv & broker
$ nohup sh mqnamesrv &
$ nohup sh bin/mqbroker -n localhost:9876 &
```

#### Edit configuration

* Config storage local cache path

```shell
$ storage.local.cache.path="" >> schema-storage-rocketmq/src/main/resources
```

#### Deployment & Running locally

Take the build JAR in target/build/schema-register.jar and run `java -cp schema-register.jar` to start service.

The REST API can be accessed from http://localhost:8080/schema-registry/v1

Swagger API documentation can be accessed from http://localhost:8080/swagger-ui/index.html

Package management
--------------

If you want to upload binary resources to your package repository like artifactory, schema-registry
support `schema.dependency.upload-enabled = true` to enable package management.

Properties details:

| Property                                | Description                                                                |
|-----------------------------------------|----------------------------------------------------------------------------|
| schema.dependency.jdk-path              | The JDK used when compiling Java files                                     |
| schema.dependency.compile-path          | The root directory used when compiling Java files                          |
| schema.dependency.local-repository-path | The local cache directory for the Jar package                              |
| schema.dependency.repository-url        | The remote repository access url, multiple repository cannot be configured |
| schema.dependency.username              | The remote repository access username                                      |
| schema.dependency.password              | The remote repository access password                                      |

**Notice: Please make sure your account has permission to upload to the remote repository.**

API Reference
--------------

```shell

# Register new schema on specified subject with default cluster and tenant
$ curl -X post -H "Content-Type: application/json" \
--data '{"details":{"schemaRecords":[{"idl":"{\"type\":\"record\",\"name\":\"Demo\",\"namespace\":\"com.schema.example\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"id\",\"type\":\"int\"}]}"}]}, "meta":{"compatibility":"BACKWARD","namespace":"com.schema.example","owner":"test","schemaName":"Demo","tenant":"default","type":"AVRO"}}' \
http://localhost:8080/schema-registry/v1/subject/{subject-name}/schema/{schema-name}

# Register new schema with cluster specified cluster and tenant
$ curl -X post -H "Content-Type: application/json" \
--data '{"details":{"schemaRecords":[{"idl":"{\"type\":\"record\",\"name\":\"Demo\",\"namespace\":\"com.schema.example\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"id\",\"type\":\"int\"}]}"}]}, "meta":{"compatibility":"BACKWARD","namespace":"com.schema.example","owner":"test","schemaName":"Demo","tenant":"default","type":"AVRO"}}' \
http://localhost:8080/schema-registry/v1/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema/{schema-name}

# Delete schema all version
$ curl -X delete http://localhost:8080/schema-registry/v1/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema

# Update schema and generate a new version, you can also use default cluster and tenant like register interface
$ curl -X put -H "Content-Type: application/json" \
--data '{"details":{"schemaRecords":[{"idl":"{\"type\":\"record\",\"name\":\"Demo\",\"namespace\":\"com.schema.example\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"type\",\"type\":\"string\"}]}"}]}, "meta":{"compatibility":"BACKWARD","namespace":"com.schema.example","owner":"test","schemaName":"Demo","tenant":"default","type":"AVRO"}}' \
http://localhost:8080/schema-registry/v1/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema/{schema-name}

# Get binding schema version by subject with specified cluster and tenant, , you can also use default cluster and tenant like register interface
$ curl -X GET http://localhost:8081/schema-registry/v1/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema

# Get schema record by specified version
$ curl -X GET http://localhost:8081/schema-registry/v1/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema/versions/{version}

# Get all schema record
$ curl -X GET http://localhost:8081/schema-registry/v1/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema/versions
```

Contribute
--------------

We always welcome new contributions, whether for trivial
cleanups, [big new features](https://github.com/apache/rocketmq/wiki/RocketMQ-Improvement-Proposal) or other material
rewards, more details see [here](http://rocketmq.apache.org/docs/how-to-contribute/).

License
----------
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Copyright (C) Apache Software Foundation

