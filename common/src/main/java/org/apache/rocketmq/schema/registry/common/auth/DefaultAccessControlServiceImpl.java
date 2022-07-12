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

package org.apache.rocketmq.schema.registry.common.auth;

import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.schema.registry.common.model.SchemaOperation;
import org.apache.rocketmq.schema.registry.common.QualifiedName;
import org.apache.rocketmq.schema.registry.common.exception.SchemaAuthorizedException;
import org.apache.rocketmq.schema.registry.common.properties.GlobalConfig;

public class DefaultAccessControlServiceImpl implements AccessControlService {

    private final GlobalConfig config;

    public DefaultAccessControlServiceImpl(final GlobalConfig config) {
        this.config = config;
    }

    @Override
    public void checkPermission(
        String role,
        String resource,
        SchemaOperation operation
    ) throws SchemaAuthorizedException {
        if (config.isAclEnabled()) {
            // TODO
            checkPermission(config.getAcl(), role, resource, operation);
        }
    }

    /**
     * Check at database level.
     */
    private void checkPermission(
        final Map<QualifiedName, Set<String>> accessACL,
        final String userName,
        final String name,
        final SchemaOperation operation
    ) {
        final Set<String> users = null;
//            accessACL.get(QualifiedName.ofDatabase(name.getCatalogName(), name.getDatabaseName()));
        if ((users != null) && !users.isEmpty() && !users.contains(userName)) {
            throw new SchemaAuthorizedException(String.format("%s is not permitted for %s %s", userName, operation.name(), name));
        }
    }
}
