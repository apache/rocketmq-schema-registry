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

import org.apache.rocketmq.schema.registry.common.properties.GlobalConfig;
import org.apache.rocketmq.schema.registry.common.storage.StorageManager;
import org.apache.rocketmq.schema.registry.core.dependency.ArtifactoryDependencyServiceImpl;
import org.apache.rocketmq.schema.registry.core.dependency.DependencyService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SchemaManagerConfig {

    /**
     * Manager of the storages.
     *
     * @param config global config
     * @return The storage manager
     */
    @Bean
    public StorageManager storageManager(final GlobalConfig config) {
        return new StorageManager(config);
    }

    /**
     * Manager of the dependencies.
     *
     * @param config global config
     * @return The storage manager
     */
    @Bean
    public DependencyService dependencyManager(final GlobalConfig config) {
        return new ArtifactoryDependencyServiceImpl(config);
    }
}
