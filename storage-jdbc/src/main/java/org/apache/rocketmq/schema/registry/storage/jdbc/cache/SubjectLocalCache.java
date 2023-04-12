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

package org.apache.rocketmq.schema.registry.storage.jdbc.cache;

import com.google.common.collect.Maps;
import org.apache.rocketmq.schema.registry.common.model.SchemaRecordInfo;

import java.util.Map;
import java.util.function.Consumer;

public class SubjectLocalCache implements LocalCache<String, SchemaRecordInfo> {

    public static Map<String, SchemaRecordInfo> subjectCache = Maps.newConcurrentMap();

    @Override
    public SchemaRecordInfo put(String subjectFullName, SchemaRecordInfo schemaRecordInfo) {
        return subjectCache.get(subjectFullName);
    }

    @Override
    public SchemaRecordInfo get(String subjectFullName, Consumer<String> consumer) {
        if (!containsKey(subjectFullName)) {
            consumer.accept(subjectFullName);
        }
        return subjectCache.get(subjectFullName);
    }

    @Override
    public SchemaRecordInfo get(String subjectFullName) {
        return subjectCache.get(subjectFullName);
    }

    @Override
    public SchemaRecordInfo remove(String s) {
        return subjectCache.remove(s);
    }

    @Override
    public boolean containsKey(String fullName) {
        return subjectCache.containsKey(fullName);
    }

    @Override
    public Map<String, SchemaRecordInfo> getCache() {
        return subjectCache;
    }
}
