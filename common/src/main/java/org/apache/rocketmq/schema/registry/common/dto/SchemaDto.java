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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.rocketmq.schema.registry.common.QualifiedName;

@ApiModel(description = "Schema detail information")
@Data
@EqualsAndHashCode(callSuper = false)
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SchemaDto extends BaseDto {
    private static final long serialVersionUID = -441512542075118183L;

    @ApiModelProperty(value = "The qualified name of this entity")
    private QualifiedName qualifiedName;

    @ApiModelProperty(value = "Information about schema changes")
    private AuditDto audit;

    @ApiModelProperty(value = "Information about schema meta", required = true)
    private SchemaMetaDto meta;

    @ApiModelProperty(value = "Information about schema details", required = true)
    private SchemaDetailDto details;

    @ApiModelProperty(value = "Information about schema persistence")
    private SchemaStorageDto storage;

    @ApiModelProperty(value = "Extra schema parameters")
    private Map<String, String> extras;

    public SchemaDto setQualifiedName(QualifiedName qualifiedName) {
        this.qualifiedName = qualifiedName;
        return this;
    }
}
