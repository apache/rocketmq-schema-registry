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

import java.util.Map;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.rocketmq.schema.registry.common.model.Dependency;

@Data
@EqualsAndHashCode(callSuper = false)
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SchemaStorageDto extends BaseDto {
    private static final long serialVersionUID = -3298771958844258686L;

    @ApiModelProperty(value = "Protocol of the schema serializer / deserializer")
    private String serdeProtocol;

    @ApiModelProperty(value = "Uploaded dependency library of the schema")
    private Dependency dependency;

    @ApiModelProperty(value = "Extra storage parameters")
    private Map<String, String> serdeInfo;

    @ApiModelProperty(value = "URI of the schema")
    private String uri;
}
