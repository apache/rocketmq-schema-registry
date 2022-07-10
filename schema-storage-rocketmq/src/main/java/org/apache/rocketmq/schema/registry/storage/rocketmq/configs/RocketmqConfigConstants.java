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

package org.apache.rocketmq.schema.registry.storage.rocketmq.configs;

import java.nio.charset.StandardCharsets;

public class RocketmqConfigConstants {

    public static final String STORAGE_TYPE = "storage.type";
    public static final String STORAGE_TYPE_DEFAULT = "rocketmq";

    public static final String STORAGE_ROCKETMQ_PRODUCER_GROUP = "storage.rocketmq.producer.group";
    public static final String STORAGE_ROCKETMQ_PRODUCER_GROUP_DEFAULT = "default";

    public static final String STORAGE_ROCKETMQ_CONSUMER_GROUP = "storage.rocketmq.consumer.group";
    // TODO : ip
    public static final String STORAGE_ROCKETMQ_CONSUMER_GROUP_DEFAULT = "default";

    public static final String STORAGE_ROCKETMQ_NAMESRV = "storage.rocketmq.namesrv";
    public static final String STORAGE_ROCKETMQ_NAMESRV_DEFAULT = "localhost:9876";

    public static final String STORAGE_ROCKETMQ_TOPIC = "storage.rocketmq.topic";
    public static final String STORAGE_ROCKETMQ_TOPIC_DEFAULT = "schema_registry_storage";

    public static final String STORAGE_LOCAL_CACHE_PATH = "storage.local.cache.path";
    public static final String STORAGE_LOCAL_CACHE_PATH_DEFAULT = "/tmp/schema-registry/cache";

    public static final byte[] STORAGE_ROCKSDB_SCHEMA_DEFAULT_FAMILY = "default".getBytes(StandardCharsets.UTF_8);
    public static final byte[] STORAGE_ROCKSDB_SCHEMA_COLUMN_FAMILY = "schema".getBytes(StandardCharsets.UTF_8);
    public static final byte[] STORAGE_ROCKSDB_SUBJECT_COLUMN_FAMILY = "subject".getBytes(StandardCharsets.UTF_8);

    public static final String DELETE_KEYS = "%DEL%";


}
