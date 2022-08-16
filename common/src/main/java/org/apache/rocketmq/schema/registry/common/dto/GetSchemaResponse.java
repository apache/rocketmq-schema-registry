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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.rocketmq.schema.registry.common.QualifiedName;
import org.apache.rocketmq.schema.registry.common.model.Dependency;
import org.apache.rocketmq.schema.registry.common.model.SchemaRecordInfo;
import org.apache.rocketmq.schema.registry.common.model.SchemaType;
import org.apache.rocketmq.schema.registry.common.utils.CommonUtil;

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

    @ApiModelProperty(value = "Schema record unique id", required = true)
    private long recordId;

    @ApiModelProperty(value = "Schema idl")
    private String idl;

    @ApiModelProperty(value = "Schema idl")
    private List<Field> fields;

    @ApiModelProperty(value = "Schema dependency")
    private Dependency dependency;

    @ApiModelProperty(value = "Schema type")
    private SchemaType type;

    public GetSchemaResponse(QualifiedName name, SchemaRecordInfo schemaRecordInfo) {
        this.subjectFullName = name.subjectFullName();
        this.schemaFullName = schemaRecordInfo.getSchema();
        this.recordId = CommonUtil.getSchemaRecordId(schemaRecordInfo.getSchemaId(),
            schemaRecordInfo.getVersion());
        this.idl = schemaRecordInfo.getIdl();
        this.dependency = schemaRecordInfo.getDependency();
        this.type = schemaRecordInfo.getType();
        this.fields = parse(idl);
    }

    private List<Field> parse(String schemaIdl) {
        Schema schema = new Schema.Parser().parse(schemaIdl);
        return schema.getFields().stream().map(field -> {
            String type = field.schema().getType().getName();
            // ["null", "double"] represent this field is nullable
            if (field.schema().isUnion() && field.schema().getTypes().size() == 2) {
                type = field.schema().getTypes().get(1).getName();
            }
            String defaultVal = field.hasDefaultValue() ? field.defaultVal().toString() : "null";
            return Field.builder()
                    .pos(field.pos())
                    .name(field.name())
                    .type(type)
                    .comment(field.doc())
                    .isNullable(field.schema().isNullable())
                    .defaultValue(defaultVal)
                    .sortType(field.order().name())
                    .extra("")
                    .build();
        }).collect(Collectors.toList());
    }
}
