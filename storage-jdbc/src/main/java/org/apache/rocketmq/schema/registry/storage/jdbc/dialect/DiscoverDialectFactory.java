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

package org.apache.rocketmq.schema.registry.storage.jdbc.dialect;

import com.google.common.collect.Maps;

import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Discover dialect factory
 */
public class DiscoverDialectFactory {

    private static final Map<String, DatabaseDialectProvider> DIALECT_CACHE = Maps.newConcurrentMap();

    static {
        loadDialects();
    }

    private static void loadDialects() {
        ServiceLoader<DatabaseDialectProvider> dialects = ServiceLoader.load(DatabaseDialectProvider.class);
        Iterator<DatabaseDialectProvider> dialectIterator = dialects.iterator();
        while (dialectIterator.hasNext()) {
            DatabaseDialectProvider dialect = dialectIterator.next();
            DIALECT_CACHE.put(dialect.databaseType(), dialect);
        }
    }

    public static DatabaseDialectProvider getDialectProvider(String dbType) {
        return DIALECT_CACHE.get(dbType);
    }
}
