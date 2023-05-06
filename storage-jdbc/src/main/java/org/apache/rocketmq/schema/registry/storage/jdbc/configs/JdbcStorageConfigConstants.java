/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.schema.registry.storage.jdbc.configs;

public class JdbcStorageConfigConstants {

    // jdbc storage config
    public final static String STORAGE_JDBC_TYPE = "storage.jdbc.type";
    public final static String STORAGE_JDBC_TYPE_MYSQL = "mysql";
    public final static String STORAGE_JDBC_URL = "storage.jdbc.url";
    public final static String STORAGE_JDBC_USER = "storage.jdbc.user";
    public final static String STORAGE_JDBC_PASSWORD = "storage.jdbc.password";
    public final static String STORAGE_JDBC_DATABASE_NAME = "storage.jdbc.database.name";
    public final static String STORAGE_JDBC_SCHEMA_NAME = "storage.jdbc.schema.name";
    public final static String STORAGE_JDBC_TABLE_NAME = "storage.jdbc.table.name";
    public final static String MAX_CONNECTIONS_ATTEMPTS = "max.connections.attempts";
    public final static String MAX_CONNECTIONS_ATTEMPTS_DEFAULT = "3";
    public final static String CONNECTION_RETRY_BACKOFF = "connection.retry.backoff";
    public final static String CONNECTION_RETRY_BACKOFF_DEFAULT = "1000";
    public final static String HAZELCAST_YAML_PATH = "storage.jdbc.hazelcast.yaml.path";
    public final static String DATABASE_DEFAULT = "schema_registry";
    public final static String TABLE_NAME_DEFAULT = "schema_table";
}
