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

package org.apache.rocketmq.schema.registry.common.dto;

import org.apache.rocketmq.schema.registry.common.QualifiedName;
import org.apache.rocketmq.schema.registry.common.model.Dependency;
import org.apache.rocketmq.schema.registry.common.model.SchemaRecordInfo;
import org.apache.rocketmq.schema.registry.common.model.SchemaType;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode(callSuper = false)
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GetSchemaResponse extends BaseDto {
    private static final long serialVersionUID = -4612593696179069203L;

    @ApiModelProperty(value = "Schema dependency")
    private String subjectFullName;

    @ApiModelProperty(value = "Schema full name")
    private String schemaFullName;

    @ApiModelProperty(value = "Schema id")
    private long schemaId;

    @ApiModelProperty(value = "Schema version")
    private long version;

    @ApiModelProperty(value = "Schema idl")
    private String idl;

    @ApiModelProperty(value = "Schema dependency")
    private Dependency dependency;

    @ApiModelProperty(value = "Schema type")
    private SchemaType type;

    public GetSchemaResponse(QualifiedName name, SchemaRecordInfo schemaRecordInfo) {
        this.subjectFullName = name.subjectFullName();
        this.schemaFullName = schemaRecordInfo.getSchema();
        this.schemaId = schemaRecordInfo.getSchemaId();
        this.version = schemaRecordInfo.getVersion();
        this.idl = schemaRecordInfo.getIdl();
        this.dependency = schemaRecordInfo.getDependency();
        this.type = schemaRecordInfo.getType();
    }

}
