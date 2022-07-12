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

package org.apache.rocketmq.schema.registry.storage.rocketmq;

import java.io.File;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.schema.registry.common.QualifiedName;
import org.apache.rocketmq.schema.registry.common.context.StoragePluginContext;
import org.apache.rocketmq.schema.registry.common.json.JsonConverter;
import org.apache.rocketmq.schema.registry.common.json.JsonConverterImpl;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;
import org.apache.rocketmq.schema.registry.common.model.SchemaRecordInfo;
import org.apache.rocketmq.schema.registry.common.utils.CommonUtil;

@Slf4j
public class RocketmqStorageClientImpl implements RocketmqStorageClient {

    private Properties storageConfig;
    private RocketmqClient rocketmqClient;
    private JsonConverter jsonConverter;

    public RocketmqStorageClientImpl(StoragePluginContext context) {
        storageConfig = CommonUtil.loadProperties(new File(context.getConfig().getStorageConfigPath()));
        rocketmqClient = new RocketmqClient(storageConfig);
        jsonConverter = new JsonConverterImpl();
    }

    /**
     * Register rocketmq schema entity.
     *
     * @param schemaInfo schema info
     */
    @Override
    public SchemaInfo register(SchemaInfo schemaInfo) {
        return rocketmqClient.registerSchema(schemaInfo);
    }

    /**
     * Delete rocketmq schema entity.
     *
     * @param qualifiedName schema name
     */
    @Override
    public void delete(QualifiedName qualifiedName) {
        rocketmqClient.delete(qualifiedName);
    }

    /**
     * Update rocketmq schema entity.
     *
     * @param schemaInfo schema info
     */
    @Override
    public SchemaInfo update(SchemaInfo schemaInfo) {
        return rocketmqClient.updateSchema(schemaInfo);
    }

    /**
     * Get rocketmq schema entity.
     *
     * @param qualifiedName schema name
     */
    @Override
    public SchemaInfo getSchema(QualifiedName qualifiedName) {
        byte[] result = rocketmqClient.getSchema(qualifiedName);
        return result == null ? null : jsonConverter.fromJson(result, SchemaInfo.class);
    }

    /**
     * Get rocketmq schema entity from subject.
     *
     * @param qualifiedName schema name
     */
    @Override
    public SchemaRecordInfo getBySubject(QualifiedName qualifiedName) {
        byte[] result = rocketmqClient.getBySubject(qualifiedName);
        return result == null ? null : jsonConverter.fromJson(result, SchemaRecordInfo.class);
    }
}
