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

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.schema.registry.common.QualifiedName;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;
import org.apache.rocketmq.schema.registry.common.model.SchemaRecordInfo;
import org.apache.rocketmq.schema.registry.common.storage.StorageService;
import org.apache.rocketmq.schema.registry.common.context.StorageServiceContext;

@Slf4j
public class RocketmqStorageService implements StorageService<SchemaInfo> {

    private final RocketmqStorageClient storageClient;

    /**
     * Constructor.
     *
     * @param storageClient rocketmq storage client
     */
    public RocketmqStorageService(RocketmqStorageClient storageClient) {
        this.storageClient = storageClient;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public SchemaInfo register(StorageServiceContext context, SchemaInfo schemaInfo) {
        return storageClient.register(schemaInfo);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void delete(StorageServiceContext context, QualifiedName name) {
        storageClient.delete(name);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public SchemaInfo update(StorageServiceContext context, SchemaInfo schemaInfo) {
        return storageClient.update(schemaInfo);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public SchemaInfo get(StorageServiceContext context, QualifiedName name) {
        return storageClient.getSchema(name);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public SchemaRecordInfo getBySubject(StorageServiceContext context, QualifiedName name) {
        return storageClient.getBySubject(name);
    }
}
