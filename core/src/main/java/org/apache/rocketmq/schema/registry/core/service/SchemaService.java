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

package org.apache.rocketmq.schema.registry.core.service;

import java.util.List;
import org.apache.rocketmq.schema.registry.common.QualifiedName;
import org.apache.rocketmq.schema.registry.common.dto.BaseDto;
import org.apache.rocketmq.schema.registry.common.dto.DeleteSchemeResponse;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaRequest;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaResponse;
import org.apache.rocketmq.schema.registry.common.dto.SchemaRecordDto;
import org.apache.rocketmq.schema.registry.common.dto.UpdateSchemaRequest;
import org.apache.rocketmq.schema.registry.common.dto.UpdateSchemaResponse;

public interface SchemaService<T extends BaseDto> {

    /**
     * Register the given schema.
     *
     * @param qualifiedName tenant / name of the schema
     * @param dto           register resource information
     * @return registered schema object
     */
    RegisterSchemaResponse register(QualifiedName qualifiedName, RegisterSchemaRequest dto);

    /**
     * Register the schema.
     *
     * @param qualifiedName tenant / name of the schema
     * @param dto           update information
     * @return updated schema object
     */
    UpdateSchemaResponse update(QualifiedName qualifiedName, UpdateSchemaRequest dto);

    /**
     * Deletes the schema.
     *
     * @param qualifiedName tenant / name of the schema
     * @return deleted schema object
     */
    DeleteSchemeResponse delete(QualifiedName qualifiedName);

    /**
     * Query the schema object with the given name.
     *
     * @param qualifiedName tenant / name of the schema
     * @return schema object with the schemaName
     */
    T get(QualifiedName qualifiedName);

    /**
     * Query the schema object with the given subject name.
     *
     * @param qualifiedName subject of the schema binding
     * @return schema object with the schemaName
     */
    GetSchemaResponse getBySubject(QualifiedName qualifiedName);

    /**
     * Query the schema object with the given subject name.
     *
     * @param qualifiedName subject of the schema binding
     * @return schema object with the schemaName
     */
    List<SchemaRecordDto> listBySubject(QualifiedName qualifiedName);

    List<String> listSubjectsByTenant(QualifiedName qualifiedName);
}
