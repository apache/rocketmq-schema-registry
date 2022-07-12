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

package org.apache.rocketmq.schema.registry.common.utils;

import org.apache.rocketmq.schema.registry.common.context.StorageServiceContext;
import org.apache.rocketmq.schema.registry.common.dto.SchemaDto;
import org.apache.rocketmq.schema.registry.common.dto.SubjectDto;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;
import org.apache.rocketmq.schema.registry.common.context.RequestContext;
import org.apache.rocketmq.schema.registry.common.dto.AuditDto;
import org.apache.rocketmq.schema.registry.common.dto.FieldDto;
import org.apache.rocketmq.schema.registry.common.dto.SchemaDetailDto;
import org.apache.rocketmq.schema.registry.common.dto.SchemaMetaDto;
import org.apache.rocketmq.schema.registry.common.dto.SchemaRecordDto;
import org.apache.rocketmq.schema.registry.common.dto.SchemaStorageDto;
import org.apache.rocketmq.schema.registry.common.model.AuditInfo;
import org.apache.rocketmq.schema.registry.common.model.FieldInfo;
import org.apache.rocketmq.schema.registry.common.model.SchemaDetailInfo;
import org.apache.rocketmq.schema.registry.common.model.SchemaMetaInfo;
import org.apache.rocketmq.schema.registry.common.model.SchemaRecordInfo;
import org.apache.rocketmq.schema.registry.common.model.SchemaStorageInfo;
import org.apache.rocketmq.schema.registry.common.model.SubjectInfo;
import org.dozer.DozerBeanMapper;
import org.dozer.Mapper;
import org.dozer.loader.api.BeanMappingBuilder;

public class StorageUtil {
    private final Mapper mapper;

    public StorageUtil() {
        final DozerBeanMapper dozerBeanMapper = new DozerBeanMapper();
        final BeanMappingBuilder builder = new BeanMappingBuilder() {
            @Override
            protected void configure() {
                mapping(SchemaDto.class, SchemaInfo.class);
                mapping(SchemaMetaDto.class, SchemaMetaInfo.class);
                mapping(SchemaDetailDto.class, SchemaDetailInfo.class);
                mapping(SchemaStorageDto.class, SchemaStorageInfo.class);
                mapping(SchemaRecordDto.class, SchemaRecordInfo.class);
                mapping(SubjectDto.class, SubjectInfo.class);
                mapping(AuditDto.class, AuditInfo.class);
                mapping(FieldDto.class, FieldInfo.class);
            }
        };
        dozerBeanMapper.addMapping(builder);
        this.mapper = dozerBeanMapper;
    }

    /**
     * Converts from SchemaDto to SchemaInfo.
     *
     * @param schemaDto schema dto
     * @return schema info
     */
    public SchemaInfo convertFromSchemaDto(final SchemaDto schemaDto) {
        return mapper.map(schemaDto, SchemaInfo.class);
    }

    /**
     * Converts from schemaInfo to SchemaDto.
     *
     * @param schemaInfo schema info
     * @return schema dto
     */
    public SchemaDto convertToSchemaDto(final SchemaInfo schemaInfo) {
        return mapper.map(schemaInfo, SchemaDto.class);
    }

    /**
     * Converts from schemaInfo to SchemaDto.
     *
     * @param recordInfo schema record info
     * @return schema dto
     */
    public SchemaRecordDto convertToSchemaRecordDto(final SchemaRecordInfo recordInfo) {
        return mapper.map(recordInfo, SchemaRecordDto.class);
    }

    /**
     * Converts to the storage service context.
     *
     * @param requestContext request context
     * @return storage service context
     */
    public StorageServiceContext convertToStorageServiceContext(final RequestContext requestContext) {
        return mapper.map(requestContext, StorageServiceContext.class);
    }

}
