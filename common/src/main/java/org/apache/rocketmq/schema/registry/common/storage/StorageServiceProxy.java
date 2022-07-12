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

package org.apache.rocketmq.schema.registry.common.storage;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.schema.registry.common.QualifiedName;
import org.apache.rocketmq.schema.registry.common.context.RequestContext;
import org.apache.rocketmq.schema.registry.common.context.StorageServiceContext;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;
import org.apache.rocketmq.schema.registry.common.context.RequestContextManager;
import org.apache.rocketmq.schema.registry.common.model.SchemaRecordInfo;
import org.apache.rocketmq.schema.registry.common.utils.StorageUtil;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;

@Slf4j
@CacheConfig(cacheNames = "storage")
public class StorageServiceProxy {

    private final StorageManager storageManager;
    private final StorageUtil storageUtil;

    /**
     * Constructor for storage service proxy.
     *
     * @param storageManager    storage manager
     * @param storageUtil       convert from Dto to storage instance or vice versa
     */
    public StorageServiceProxy(final StorageManager storageManager, final StorageUtil storageUtil) {
        this.storageManager = storageManager;
        this.storageUtil = storageUtil;
    }

    /**
     * Proxy calls the StorageService's register method.
     * @param qualifiedName Qualified name with tenant / name of schema
     * @param schemaInfo schema object
     */
    public SchemaInfo register(
        final QualifiedName qualifiedName,
        final SchemaInfo schemaInfo
    ) {
        final RequestContext requestContext = RequestContextManager.getContext();
        final StorageServiceContext storageContext = storageUtil.convertToStorageServiceContext(requestContext);
        final StorageService<SchemaInfo> storageService = storageManager.getStorageService();

        return storageService.register(storageContext, schemaInfo);
    }

    /**
     * Proxy calls the StorageService's register method.
     *
     * @param name Qualified name with tenant / name of schema
     */
    @CacheEvict(key = "'schema.' + #name.getTenant() + '/' + #name.schema()")
    public void delete(final QualifiedName name) {
        final RequestContext requestContext = RequestContextManager.getContext();
        final StorageServiceContext storageServiceContext = storageUtil.convertToStorageServiceContext(requestContext);
        final StorageService<SchemaInfo> storageService = storageManager.getStorageService();

        storageService.delete(storageServiceContext, name);
    }

    /**
     * Proxy calls the StorageService's update method.
     *
     * @param name schema qualified name
     * @param schemaInfo schema information instance
     * @return true if errors after this should be ignored.
     */
    @CacheEvict(key = "'schema.' + #name.getTenant() + '/' + #name.schema()")
    public SchemaInfo update(final QualifiedName name, final SchemaInfo schemaInfo) {
        final RequestContext requestContext = RequestContextManager.getContext();
        final StorageServiceContext storageServiceContext = storageUtil.convertToStorageServiceContext(requestContext);
        final StorageService<SchemaInfo> service = storageManager.getStorageService();

        return service.update(storageServiceContext, schemaInfo);
    }

    /**
     * Proxy calls the StorageService's get method. Returns schema from store
     * if <code>useCache</code> is false.
     *
     * @param name Qualified name with tenant / name of schema
     * @param useCache if schema can be retrieved from cache
     * @return schema information instance
     */
    @Cacheable(key = "'schema.' + #name.getTenant() + '/' + #name.schema()", condition = "#useCache")
    public SchemaInfo get(final QualifiedName name, final boolean useCache) {
        final RequestContext requestContext = RequestContextManager.getContext();
        final StorageServiceContext storageServiceContext = storageUtil.convertToStorageServiceContext(requestContext);
        final StorageService<SchemaInfo> storageService = storageManager.getStorageService();

        return storageService.get(storageServiceContext, name);
    }

    @Cacheable(key = "'subject.' + #subject", condition = "#useCache")
    public SchemaRecordInfo getBySubject(final QualifiedName name, final boolean useCache) {
        final RequestContext requestContext = RequestContextManager.getContext();
        final StorageServiceContext storageServiceContext = storageUtil.convertToStorageServiceContext(requestContext);
        final StorageService<SchemaInfo> storageService = storageManager.getStorageService();

        return storageService.getBySubject(storageServiceContext, name);
    }
}
