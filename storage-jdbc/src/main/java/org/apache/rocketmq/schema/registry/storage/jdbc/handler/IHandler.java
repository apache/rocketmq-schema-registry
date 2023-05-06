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

package org.apache.rocketmq.schema.registry.storage.jdbc.handler;

import org.apache.rocketmq.schema.registry.common.QualifiedName;
import org.apache.rocketmq.schema.registry.common.context.StorageServiceContext;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;
import org.apache.rocketmq.schema.registry.common.model.SchemaRecordInfo;

import java.io.Closeable;
import java.util.List;

public abstract class IHandler implements Closeable {
    /**
     * Register schema
     *
     * @param schema
     */
    public abstract void register(SchemaInfo schema);

    /**
     * update schema
     *
     * @param schema
     */
    public abstract void updateSchema(SchemaInfo schema);

    /**
     * delete schema
     *
     * @param qualifiedName
     */
    public abstract void deleteSchema(QualifiedName qualifiedName);

    /**
     * delete schema
     *
     * @param qualifiedName
     */
    public abstract void deleteBySubject(QualifiedName qualifiedName);

    /**
     * delete by version
     *
     * @param name
     */
    public abstract void deleteByVersion(QualifiedName name);

    /**
     * get schema
     *
     * @param qualifiedName
     * @return
     */
    public abstract SchemaInfo getSchema(QualifiedName qualifiedName);

    /**
     * get by subject
     *
     * @param subjectFullName
     * @return
     */
    public abstract SchemaRecordInfo getBySubject(String subjectFullName);

    /**
     * Get schema info by subject
     *
     * @param subjectFullName
     * @return
     */
    public abstract SchemaInfo getSchemaInfoBySubject(String subjectFullName);

    /**
     * Get subjects
     *
     * @param context
     * @param tenant
     * @return
     */
    public abstract List<String> getSubjects(StorageServiceContext context, String tenant);

    /**
     * Get tenants
     *
     * @param cluster
     * @return
     */
    public abstract List<String> getTenants(String cluster);


    protected void changeNotify() {

    }
}
