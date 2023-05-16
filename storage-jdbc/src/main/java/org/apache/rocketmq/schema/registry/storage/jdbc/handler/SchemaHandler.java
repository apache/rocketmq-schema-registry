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

package org.apache.rocketmq.schema.registry.storage.jdbc.handler;

import com.google.common.collect.Sets;
import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.schema.registry.common.QualifiedName;
import org.apache.rocketmq.schema.registry.common.constant.SchemaConstants;
import org.apache.rocketmq.schema.registry.common.context.StorageServiceContext;
import org.apache.rocketmq.schema.registry.common.exception.SchemaException;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;
import org.apache.rocketmq.schema.registry.common.model.SchemaRecordInfo;
import org.apache.rocketmq.schema.registry.common.model.SubjectInfo;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.rocketmq.schema.registry.storage.jdbc.store.JdbcSchemaMapStore.SCHEMAS;
import static org.apache.rocketmq.schema.registry.storage.jdbc.store.JdbcSubjectMapStore.SUBJECTS;

@Slf4j
public class SchemaHandler extends IHandler {
    private final IMap<String, SchemaInfo> schemas;
    private final IMap<String, SchemaRecordInfo> subjects;
    private final HazelcastInstance hazelcastInstance;

    public SchemaHandler(String hazelcastYamlConfigPath) {
        Config config;
        try {
            config = Config.loadFromFile(new File(hazelcastYamlConfigPath));
        } catch (FileNotFoundException e) {
            throw new SchemaException(String.format("File [%s] not found", hazelcastYamlConfigPath), e);
        }

        this.hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        this.subjects = this.hazelcastInstance.getMap(SUBJECTS);
        this.subjects.loadAll(true);

        this.schemas = this.hazelcastInstance.getMap(SCHEMAS);
        this.schemas.loadAll(true);
        this.schemas.addEntryListener(new SchemaChangeEntryListener(this.subjects), true);
        loadAllSubject();
    }

    private void loadAllSubject() {
        for (Map.Entry<String, SchemaInfo> schema : schemas.entrySet()) {
            SchemaInfo schemaInfo = schema.getValue();
            List<SchemaRecordInfo> allSchemaRecords = schemaInfo.getDetails().getSchemaRecords();
            for (SchemaRecordInfo record : allSchemaRecords) {
                List<String> recordSubjects =
                        record.getSubjects().stream().map(SubjectInfo::fullName).collect(Collectors.toList());
                recordSubjects.forEach(subject -> {
                    subjects.put(subject, record);
                });
            }
        }
    }

    @Override
    public void register(SchemaInfo schema) {
        if (schemas.containsKey(schema.schemaFullName())) {
            log.error(String.format("Schema %s already exists, registration failed", schema.schemaFullName()));
            return;
        }
        schemas.put(schema.schemaFullName(), schema);
    }

    /**
     * update schema
     *
     * @param update
     */
    @Override
    public void updateSchema(SchemaInfo update) {
        if (!schemas.containsKey(update.schemaFullName())) {
            log.warn(String.format("Schema %s does not exist, update failed", update.schemaFullName()));
            return;
        }
        // Get lock
        schemas.lock(update.schemaFullName());
        try {
            SchemaInfo current = schemas.get(update.schemaFullName());
            boolean hasVersionDeleted = current.getRecordCount() > update.getRecordCount();
            if (current.getLastModifiedTime() != null && update.getLastModifiedTime() != null &&
                    current.getLastModifiedTime().after(update.getLastModifiedTime())) {
                log.info("Current Schema is later version, no need to update.");
                return;
            }
            if (current.getLastRecordVersion() == update.getLastRecordVersion() && !hasVersionDeleted) {
                log.info("Schema version is the same, no need to update.");
                return;
            }
            if (current.getLastRecordVersion() > update.getLastRecordVersion() && !hasVersionDeleted) {
                throw new SchemaException("Schema version is invalid, update: "
                        + update.getLastRecordVersion() + ", but current: " + current.getLastRecordVersion());
            }
            schemas.put(update.schemaFullName(), update);
        } finally {
            // unlock
            schemas.unlock(update.schemaFullName());
        }
    }

    @Override
    public void deleteSchema(QualifiedName qualifiedName) {
        schemas.lock(qualifiedName.schemaFullName());
        try {
            if (!schemas.containsKey(qualifiedName.schemaFullName())) {
                log.error(String.format("Schema %s does not exist, delete failed", qualifiedName.schemaFullName()));
                return;
            }
            schemas.delete(qualifiedName);
        } finally {
            schemas.unlock(qualifiedName.schemaFullName());
        }
    }

    @Override
    public void deleteBySubject(QualifiedName qualifiedName) {
        schemas.lock(qualifiedName.subjectFullName());
        try {
            SchemaInfo schemaInfo = getSchemaInfoBySubject(qualifiedName.subjectFullName());
            if (schemaInfo == null) {
                log.error(String.format("Schema %s does not exist, delete failed",
                    qualifiedName.subjectFullName()));
                return;
            }
            schemas.delete(schemaInfo.schemaFullName());
        } finally {
            schemas.unlock(qualifiedName.subjectFullName());
        }
    }

    @Override
    public void deleteByVersion(QualifiedName name) {
        SchemaInfo schemaInfo = getSchemaInfoBySubject(name.subjectFullName());
        if (schemaInfo == null || schemaInfo.getDetails() == null || schemaInfo.getDetails().getSchemaRecords() == null) {
            log.error(String.format("Schema %s does not exist, failed to delete according to version",
                name.subjectFullName()));
            return;
        }
        List<SubjectInfo> subjects = schemaInfo.getLastRecord().getSubjects();
        List<SchemaRecordInfo> schemaRecords = schemaInfo.getDetails().getSchemaRecords();
        schemaRecords.removeIf(record -> record.getVersion() == name.getVersion());
        if (CollectionUtils.isEmpty(schemaRecords)) {
            deleteBySubject(name);
        }
        if (schemaInfo.getLastRecord().getSubjects().isEmpty()) {
            schemaInfo.getLastRecord().setSubjects(subjects);
        }
        updateSchema(schemaInfo);
    }

    @Override
    public SchemaInfo getSchema(QualifiedName qualifiedName) {
        return schemas.get(qualifiedName.schemaFullName());
    }

    @Override
    public SchemaRecordInfo getBySubject(String subjectFullName) {
        subjects.lock(subjectFullName);
        try {
            if (!subjects.containsKey(subjectFullName)) {
                return null;
            }
            return subjects.get(subjectFullName);
        } finally {
            subjects.unlock(subjectFullName);
        }
    }

    @Override
    public SchemaInfo getSchemaInfoBySubject(String subjectFullName) {
        SchemaRecordInfo subjectRecordInfo = subjects.get(subjectFullName);
        if (subjectRecordInfo == null) {
            return null;
        }
        String schemaFullName = subjectRecordInfo.getSchema();
        return schemas.get(schemaFullName);
    }

    @Override
    public List<String> getSubjects(StorageServiceContext context, String tenant) {
        List<String> allSubjects = new ArrayList<>();
        for (Map.Entry<String, SchemaRecordInfo> schemaRecordEntry : subjects.entrySet()) {
            String subjectFullName = schemaRecordEntry.getKey();
            String[] subjectFromCache = subjectFullName.split(String.valueOf(SchemaConstants.SUBJECT_SEPARATOR));
            String tenantFromKey = subjectFromCache[1];
            String subjectFromKey = subjectFromCache[2];
            if (tenant.equals(tenantFromKey)) {
                allSubjects.add(subjectFromKey);
            }
        }
        return allSubjects;
    }

    @Override
    public Set<String> getTenants(String cluster) {
        Set<String> tenants = Sets.newHashSet();
        for (Map.Entry<String, SchemaRecordInfo> schemaRecordEntry : subjects.entrySet()) {
            String subjectFullName = schemaRecordEntry.getKey();
            String tenant = subjectFullName.split(String.valueOf(SchemaConstants.SUBJECT_SEPARATOR))[1];
            tenants.add(tenant);
        }
        return tenants;
    }

    @Override
    public void close() throws IOException {
        Hazelcast.shutdownAll();
    }


    /**
     * Schema change entry listener
     */
    static class SchemaChangeEntryListener implements EntryAddedListener<String, SchemaInfo>, EntryRemovedListener<String, SchemaInfo>, EntryUpdatedListener<String, SchemaInfo> {

        private final IMap<String, SchemaRecordInfo> subjectCache;

        SchemaChangeEntryListener(IMap<String, SchemaRecordInfo> localCache) {
            this.subjectCache = localCache;
        }

        @Override
        public void entryAdded(EntryEvent<String, SchemaInfo> current) {
            SchemaInfo schemaInfo = current.getValue();
            subjectCache.put(schemaInfo.subjectFullName(), schemaInfo.getLastRecord());
        }

        @Override
        public void entryRemoved(EntryEvent<String, SchemaInfo> current) {
            if (current == null || current.getValue() == null || current.getValue().getDetails() == null) {
                return;
            }
            // Delete subjects bind to any version
            List<SchemaRecordInfo> allSchemaRecords = current.getValue().getDetails().getSchemaRecords();
            List<String> allSubjects = allSchemaRecords.parallelStream()
                    .flatMap(record -> record.getSubjects().stream().map(SubjectInfo::fullName))
                    .collect(Collectors.toList());

            allSubjects.forEach(subject -> {
                subjectCache.remove(subject);
            });
        }

        @Override
        public void entryUpdated(EntryEvent<String, SchemaInfo> update) {
            if (update == null || update.getValue() == null || update.getValue().getLastRecord() == null) {
                return;
            }
            SchemaRecordInfo lastSchemaRecordInfo = update.getValue().getLastRecord();
            lastSchemaRecordInfo.getSubjects().forEach(subject -> {
                subjectCache.put(subject.fullName(), update.getValue().getLastRecord());
            });
        }
    }

}
