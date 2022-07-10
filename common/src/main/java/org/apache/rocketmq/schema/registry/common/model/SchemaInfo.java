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

package org.apache.rocketmq.schema.registry.common.model;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@Builder
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
@NoArgsConstructor
public class SchemaInfo extends BaseInfo {
    private static final long serialVersionUID = -5143258312429353896L;

    private SchemaMetaInfo meta;

    private SchemaDetailInfo details;

    private SchemaStorageInfo storage;

    private Map<String, String> extras;

    public String getSchemaName() {
        return getQualifiedName().getSchema();
    }

    public String getNamespace() {
        return getMeta().getNamespace();
    }

    public SchemaType getSchemaType() {
        return getMeta().getType();
    }

    public SchemaRecordInfo getLastRecord() {
        return getDetails().lastRecord();
    }

    public String getLastRecordIdl() {
        return getLastRecord().getIdl();
    }

    public long getUniqueId() {
        return getMeta().getUniqueId();
    }

    public void setUniqueId(long uniqueId) {
        getMeta().setUniqueId(uniqueId);
        getLastRecord().setSchemaId(uniqueId);
    }

    public void setLastRecordDependency(Dependency dependency) {
        getLastRecord().setDependency(dependency);
    }

    public long getLastRecordVersion() {
        return getLastRecord().getVersion();
    }

    public void setLastRecordVersion(long version) {
        getLastRecord().setVersion(version);
    }

}
