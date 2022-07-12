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
import org.apache.rocketmq.schema.registry.common.QualifiedName;
import org.apache.rocketmq.schema.registry.common.model.StorageType;

public interface GlobalConfig {

    /**
     * Enable schema storage cache.
     *
     * @return true if cache is enabled
     */
    boolean isCacheEnabled();

    /**
     * Enable schema upload to remote repository.
     *
     * @return true if cache is enabled
     */
    boolean isUploadEnabled();

    /**
     * File path for upload.
     *
     * @return file path
     */
    String getDependencyCompilePath();

    /**
     * File path for upload.
     *
     * @return file path
     */
    String getDependencyLocalRepositoryPath();

    /**
     * Jdk path for compile.
     *
     * @return jdk path
     */
    String getDependencyJdkPath();

    /**
     * Remote repository url.
     *
     * @return repository url
     */
    String getDependencyRepositoryUrl();

    /**
     * Remote repository username.
     *
     * @return repository username
     */
    String getDependencyUsername();

    /**
     * Remote repository password.
     *
     * @return repository password
     */
    String getDependencyPassword();

    /**
     * Upload template.
     *
     * @return Upload template
     */
    String getDependencyTemplate();

    /**
     * Enable schema acl.
     *
     * @return true if acl is enabled
     */
    boolean isAclEnabled();

    /**
     * Schema acl map.
     *
     * @return schema acl map
     */
    Map<QualifiedName, Set<String>> getAcl();

    /**
     * Schema storage layer type.
     *
     * @return schema type
     */
    StorageType getStorageType();

    /**
     * Schema storage config file.
     *
     * @return storage config file
     */
    String getStorageConfigPath();

    /**
     * Schema service region id.
     *
     * @return service region id
     */
    long getServiceRegionId();

    /**
     * Schema service node id
     *
     * @return service node id
     */
    long getServiceNodeId();
}
