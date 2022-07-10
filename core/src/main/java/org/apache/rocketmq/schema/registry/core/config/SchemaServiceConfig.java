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

package org.apache.rocketmq.schema.registry.core.config;

import org.apache.rocketmq.schema.registry.common.auth.AccessControlService;
import org.apache.rocketmq.schema.registry.common.auth.DefaultAccessControlServiceImpl;
import org.apache.rocketmq.schema.registry.common.dto.SchemaDto;
import org.apache.rocketmq.schema.registry.common.properties.GlobalConfig;
import org.apache.rocketmq.schema.registry.common.utils.IdGenerator;
import org.apache.rocketmq.schema.registry.common.utils.SnowFlakeIdGenerator;
import org.apache.rocketmq.schema.registry.core.dependency.DependencyService;
import org.apache.rocketmq.schema.registry.common.storage.StorageManager;
import org.apache.rocketmq.schema.registry.core.service.SchemaInitializationService;
import org.apache.rocketmq.schema.registry.core.service.SchemaService;
import org.apache.rocketmq.schema.registry.core.service.SchemaServiceImpl;
import org.apache.rocketmq.schema.registry.common.storage.StorageServiceProxy;
import org.apache.rocketmq.schema.registry.common.utils.StorageUtil;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SchemaServiceConfig {

    @Bean
    public SchemaService<SchemaDto> schemaService(
        final GlobalConfig config,
        final AccessControlService accessController,
        final StorageServiceProxy storageServiceProxy,
        final StorageUtil storageUtil,
        final DependencyService dependencyService,
        final IdGenerator idGenerator
    ) {
        return new SchemaServiceImpl(
            config,
            accessController,
            storageServiceProxy,
            storageUtil,
            dependencyService,
            idGenerator
        );
    }

    /**
     * Handle startup and shutdown of schema service.
     *
     * @param storageManager  Plugin manager to use
     *
     * @return The initialization service bean
     */
    @Bean
    public SchemaInitializationService schemaInitializationService(StorageManager storageManager) {
        return new SchemaInitializationService(storageManager);
    }

    /**
     * Access controller.
     *
     * @param config global config
     * @return authorization class based on config
     */
    @Bean
    @ConditionalOnMissingBean(AccessControlService.class)
    public AccessControlService accessController(final GlobalConfig config) {
        return new DefaultAccessControlServiceImpl(config);
    }

    /**
     * The storage service proxy bean.
     *
     * @param storageManager storage manager
     * @param storageUtil    storage utilities
     * @return The schema service proxy bean
     */
    @Bean
    public StorageServiceProxy storageServiceProxy(
        final StorageManager storageManager,
        final StorageUtil storageUtil
    ) {
        return new StorageServiceProxy(storageManager, storageUtil);
    }

    /**
     * The id generator of the service.
     *
     * @param config global config
     * @return The id generator
     */
    @Bean
    public IdGenerator idGenerator(final GlobalConfig config) {
        return new SnowFlakeIdGenerator(config);
    }


}
