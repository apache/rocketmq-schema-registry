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

package org.apache.rocketmq.schema.registry.storage.jdbc.store;

import com.google.common.collect.Maps;
import com.hazelcast.core.HazelcastInstance;
import org.apache.rocketmq.schema.registry.common.model.SchemaRecordInfo;
import org.apache.rocketmq.schema.registry.storage.jdbc.cache.SubjectLocalCache;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Jdbc subject map storage
 */
public class JdbcSubjectMapStore implements IMapStore<String, SchemaRecordInfo> {
    public final static String SUBJECTS = "subjects";
    private SubjectLocalCache subjectLocalCache;

    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapKey) {
        Assert.isTrue(mapKey.equals(SUBJECTS), "Subject map key should be [subjects]");
        this.subjectLocalCache = new SubjectLocalCache();
    }

    @Override
    public void store(String subjectFullName, SchemaRecordInfo schemaRecordInfo) {
        subjectLocalCache.put(subjectFullName, schemaRecordInfo);
    }

    @Override
    public void storeAll(Map<String, SchemaRecordInfo> records) {
        records.forEach((subjectFullName, record) -> {
            store(subjectFullName, record);
        });
    }

    @Override
    public void delete(String subjectFullName) {
        subjectLocalCache.remove(subjectFullName);
    }

    @Override
    public void deleteAll(Collection<String> records) {
        Iterator<String> subjects = records.iterator();
        while (subjects.hasNext()) {
            delete(subjects.next());
        }
    }

    @Override
    public SchemaRecordInfo load(String subjectFullName) {
        return subjectLocalCache.get(subjectFullName);
    }

    @Override
    public Map<String, SchemaRecordInfo> loadAll(Collection<String> records) {
        Map<String, SchemaRecordInfo> schemaRecordInfoMap = Maps.newConcurrentMap();
        Iterator<String> subjects = records.iterator();
        while (subjects.hasNext()) {
            String subject = subjects.next();
            schemaRecordInfoMap.put(subject, load(subject));
        }
        return schemaRecordInfoMap;
    }

    @Override
    public Iterable<String> loadAllKeys() {
        return subjectLocalCache.getCache().keySet();
    }

    @Override
    public void destroy() {
        this.subjectLocalCache = null;
    }
}
