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

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.schema.registry.common.QualifiedName;
import org.apache.rocketmq.schema.registry.common.exception.SchemaException;

@Data
public class AclProperties {
    private boolean enabled;
    private String aclStr;
    private Map<QualifiedName, Set<String>> aclMap;

    public Map<QualifiedName, Set<String>> getAclMap() {
        if (aclMap == null) {
            aclMap = aclStr == null ? new HashMap<>() : buildAclMap(aclStr);
        }
        return aclMap;
    }

    /**
     * Parse the configuration to get operation control. The control is at userName level
     * and the controlled operations include create, delete, and rename for table.
     * The format is below.
     * db1:user1,user2|db2:user1,user2
     *
     * @param aclStr the config strings for dbs
     * @return acl config
     */
    @VisibleForTesting
    private static Map<QualifiedName, Set<String>> buildAclMap(final String aclStr) {
        final Map<QualifiedName, Set<String>> aclMap = new HashMap<>();
        try {
            for (String entity : StringUtils.split(aclStr, "|")) {

            }
        } catch (Exception e) {
            throw new SchemaException("Schema acl property parsing error", e);
        }
        return aclMap;
    }
}
