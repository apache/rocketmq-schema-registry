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

import java.io.Serializable;
import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

@Data
@EqualsAndHashCode(callSuper = false)
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class AuditInfo implements Serializable {
    private static final long serialVersionUID = 2258089775496856662L;

    private String desc;
    private String createdBy;
    private Date createdTime;
    private String lastModifiedBy;
    private Date lastModifiedTime;

    public void createBy(String user, String desc) {
        this.desc = desc;
        this.createdBy = user;
        this.lastModifiedBy = user;
        this.createdTime = this.lastModifiedTime = new Date(System.currentTimeMillis());
    }

    public void updateBy(String user, String desc) {
        if (StringUtils.isNotBlank(user)) {
            this.lastModifiedBy = user;
        }
        if (StringUtils.isNotBlank(desc)) {
            this.desc = desc;
        }
        this.lastModifiedTime = new Date(System.currentTimeMillis());
    }
}
