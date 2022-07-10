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
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@ApiModel(description = "Schema field/column information")
@SuppressWarnings("unused")
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@Builder
@AllArgsConstructor
public class FieldDto extends BaseDto {
    private static final long serialVersionUID = -8336499483006254487L;

    @ApiModelProperty(value = "Position of the field")
    private Integer pos;

    @ApiModelProperty(value = "Name of the field", required = true)
    private String name;

    @ApiModelProperty(value = "Type of the field", required = true)
    private String type;

    @ApiModelProperty(value = "Comment of the field")
    private String comment;

    @ApiModelProperty(value = "Can the field be null, default is true")
    private Boolean isNullable = true;

    @ApiModelProperty(value = "Size of the field")
    private Integer size;

    @ApiModelProperty(value = "Default value of the field")
    private String defaultValue;

    @ApiModelProperty(value = "Is a sorted field, default is false")
    private Boolean isSortable = false;

    @ApiModelProperty(value = "This filed sorted type, like：ascending, descending, ignore")
    private String sortType;

    @ApiModelProperty(value = "Extra info of the field")
    private String extra;
}
