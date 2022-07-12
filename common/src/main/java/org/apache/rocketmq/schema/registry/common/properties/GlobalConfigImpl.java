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

package org.apache.rocketmq.schema.registry.common.properties;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.schema.registry.common.QualifiedName;
import org.apache.rocketmq.schema.registry.common.model.StorageType;

@Slf4j
public class GlobalConfigImpl implements GlobalConfig {

    private final SchemaProperties schemaProperties;

    public GlobalConfigImpl(@Nonnull @NonNull final SchemaProperties schemaProperties) {
        this.schemaProperties = schemaProperties;
    }

    @Override
    public boolean isCacheEnabled() {
        return schemaProperties.getCache().isEnabled();
    }

    @Override
    public boolean isUploadEnabled() {
        return schemaProperties.getDependency().isUploadEnabled();
    }

    @Override
    public String getDependencyCompilePath() {
        return schemaProperties.getDependency().getCompilePath();
    }

    @Override
    public String getDependencyLocalRepositoryPath() {
        return schemaProperties.getDependency().getLocalRepositoryPath();
    }

    @Override
    public String getDependencyJdkPath() {
        return schemaProperties.getDependency().getJdkPath();
    }

    @Override
    public String getDependencyRepositoryUrl() {
        return schemaProperties.getDependency().getRepositoryUrl();
    }

    @Override
    public String getDependencyUsername() {
        return schemaProperties.getDependency().getUsername();
    }

    @Override
    public String getDependencyPassword() {
        return schemaProperties.getDependency().getPassword();
    }

    public String getDependencyTemplate() {
        return schemaProperties.getDependency().getTemplate();
    }

    @Override
    public boolean isAclEnabled() {
        return schemaProperties.getAcl().isEnabled();
    }

    @Override
    public Map<QualifiedName, Set<String>> getAcl() {
        return schemaProperties.getAcl().getAclMap();
    }

    @Override
    public StorageType getStorageType() {
        return schemaProperties.getStorage().getType();
    }

    @Override
    public String getStorageConfigPath() {
        return schemaProperties.getStorage().getConfigPath();
    }

    @Override
    public long getServiceRegionId() {
        return schemaProperties.getService().getRegionId();
    }

    @Override
    public long getServiceNodeId() {
        return schemaProperties.getService().getNodeId();
    }
}
