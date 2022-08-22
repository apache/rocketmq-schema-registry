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

package org.apache.rocketmq.schema.registry.common;

import java.io.Serializable;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.rocketmq.schema.registry.common.constant.SchemaConstants;
import org.apache.rocketmq.schema.registry.common.model.SubjectInfo;

@Data
@Builder
@NoArgsConstructor
public class QualifiedName implements Serializable {
    private static final long serialVersionUID = 2266514833942841209L;

    private String cluster;
    private String tenant;
    private String subject;
    private String schema;
    private Long version;

    public QualifiedName(
        @Nullable final String cluster,
        @Nullable final String tenant,
        @Nullable final String subject,
        @Nullable final String schema
    ) {
        this.cluster = cluster;
        this.tenant = tenant;
        this.subject = subject;
        this.schema = schema;
    }

    public QualifiedName(
        @Nullable final String cluster,
        @Nullable final String tenant,
        @Nullable final String subject,
        @Nullable final String schema,
        @Nullable final Long version
    ) {
        this.cluster = cluster;
        this.tenant = tenant;
        this.subject = subject;
        this.schema = schema;
        this.version = version;
    }

    public SubjectInfo subjectInfo() {
        return new SubjectInfo(cluster, tenant, subject);
    }

    public String fullName() {
        return cluster + SchemaConstants.SUBJECT_SEPARATOR + tenant
                + SchemaConstants.SUBJECT_SEPARATOR + subject
                + SchemaConstants.SUBJECT_SEPARATOR + schema;
    }

    public String schemaFullName() {
        return tenant + SchemaConstants.SUBJECT_SEPARATOR + schema;
    }

    public String subjectFullName() {
        return cluster + SchemaConstants.SUBJECT_SEPARATOR + tenant
                + SchemaConstants.SUBJECT_SEPARATOR + subject;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("(");
        sb.append("cluster=")
            .append(cluster).append(", ");
        sb.append("tenant=")
            .append(tenant).append(", ");
        sb.append("subject=")
            .append(subject).append(", ");
        sb.append("name=")
            .append(schema).append(", ");
        sb.append("version=")
            .append(version).append(")");
        return sb.toString();
    }
}
