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

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.rocketmq.schema.registry.common.model.Compatibility;
import org.apache.rocketmq.schema.registry.common.model.SchemaType;

@Data
@EqualsAndHashCode(callSuper = false)
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RegisterSchemaRequest extends BaseDto {
    private static final long serialVersionUID = 7890248374919863930L;

    @ApiModelProperty(value = "First IDL of this schema", example = "{\"type\": \"string\"}", required = true)
    private String schemaIdl;

    @ApiModelProperty(value = "Schema type")
    private SchemaType schemaType = SchemaType.AVRO;

    @ApiModelProperty(value = "Schema owner", example = "li")
    private String owner = "";

    @ApiModelProperty(value = "Schema compatibility")
    private Compatibility compatibility = Compatibility.BACKWARD;

    @ApiModelProperty(value = "Schema description", example = "my first schema")
    private String desc = "";

}
