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

package org.apache.rocketmq.schema.registry.storage.jdbc;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.rocketmq.schema.registry.common.QualifiedName;
import org.apache.rocketmq.schema.registry.common.context.StoragePluginContext;
import org.apache.rocketmq.schema.registry.common.context.StorageServiceContext;
import org.apache.rocketmq.schema.registry.common.exception.SchemaException;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;
import org.apache.rocketmq.schema.registry.common.model.SchemaMetaInfo;
import org.apache.rocketmq.schema.registry.common.model.SchemaRecordInfo;
import org.apache.rocketmq.schema.registry.common.model.SchemaType;
import org.apache.rocketmq.schema.registry.common.storage.StorageService;
import org.apache.rocketmq.schema.registry.storage.jdbc.handler.IHandler;
import org.apache.rocketmq.schema.registry.storage.jdbc.handler.SchemaHandler;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * mysql storage service
 */
public class JdbcStorageService
    implements StorageService<SchemaInfo> {
    private final String hazelcastYamlConfigPath;
    private final IHandler handler;

    public JdbcStorageService(StoragePluginContext context) {
        this.hazelcastYamlConfigPath = context.getConfig().getStorageConfigPath();
        this.handler = new SchemaHandler(this.hazelcastYamlConfigPath);
    }

    @Override
    public SchemaInfo register(StorageServiceContext context, SchemaInfo schemaInfo) {
        try {
            handler.register(schemaInfo);
            return schemaInfo;
        } catch (Exception e) {
            throw new SchemaException("Registry schema failed", e);
        }
    }

    @Override
    public void delete(StorageServiceContext context, QualifiedName name) {
        if (name.getVersion() == null) {
            handler.deleteBySubject(name);
        } else {
            handler.deleteByVersion(name);
        }
    }

    @Override
    public SchemaInfo update(StorageServiceContext context, SchemaInfo schemaInfo) {
        handler.updateSchema(schemaInfo);
        return schemaInfo;
    }

    @Override
    public SchemaInfo get(StorageServiceContext context, QualifiedName qualifiedName) {
        return handler.getSchema(qualifiedName);
    }

    @Override
    public SchemaRecordInfo getBySubject(StorageServiceContext context, QualifiedName qualifiedName) {
        if (qualifiedName.getVersion() == null) {
            SchemaRecordInfo result = handler.getBySubject(qualifiedName.subjectFullName());
            return result;
        }
        // schema version is given
        SchemaInfo schemaInfo = handler.getSchemaInfoBySubject(qualifiedName.subjectFullName());
        if (schemaInfo == null || schemaInfo.getDetails() == null || schemaInfo.getDetails().getSchemaRecords() == null) {
            return null;
        }
        Map<Long, SchemaRecordInfo> versionSchemaMap = schemaInfo.getDetails().getSchemaRecords()
            .stream().collect(Collectors.toMap(SchemaRecordInfo::getVersion, Function.identity()));
        return versionSchemaMap.get(qualifiedName.getVersion());
    }

    @Override
    public SchemaRecordInfo getTargetSchema(StorageServiceContext context, QualifiedName qualifiedName) {
        // schema version is given
        SchemaInfo schemaInfo = handler.getSchemaInfoBySubject(qualifiedName.subjectFullName());
        if (schemaInfo == null || schemaInfo.getDetails() == null || schemaInfo.getDetails().getSchemaRecords() == null) {
            return null;
        }
        SchemaMetaInfo schemaMetaInfo = schemaInfo.getMeta();
        if (schemaMetaInfo == null) {
            return null;
        }
        if (schemaMetaInfo.getType() == SchemaType.AVRO) {
            for (SchemaRecordInfo schemaRecordInfo : schemaInfo.getDetails().getSchemaRecords()) {
                Schema store = new Schema.Parser().parse(schemaRecordInfo.getIdl());
                Schema target = new Schema.Parser().parse(qualifiedName.getSchema());
                if (Objects.equals(store, target)) {
                    return schemaRecordInfo;
                }
            }
        } else {
            //todo support other type
            return null;
        }
        return null;
    }

    @Override
    public List<SchemaRecordInfo> listBySubject(StorageServiceContext context, QualifiedName qualifiedName) {
        SchemaInfo schemaInfo = handler.getSchemaInfoBySubject(qualifiedName.subjectFullName());
        if (schemaInfo == null || schemaInfo.getDetails() == null) {
            return null;
        }
        return schemaInfo.getDetails().getSchemaRecords();
    }

    @Override
    public List<String> listSubjectsByTenant(StorageServiceContext context, QualifiedName qualifiedName) {
        return handler.getSubjects(context, qualifiedName.getTenant());
    }

    @Override
    public List<String> listTenants(StorageServiceContext storageService, QualifiedName qualifiedName) {
        return Lists.newArrayList(handler.getTenants(qualifiedName.getCluster()));
    }

    @Override
    public void close() throws IOException {
        if (this.handler != null) {
            this.handler.close();
        }
    }
}
