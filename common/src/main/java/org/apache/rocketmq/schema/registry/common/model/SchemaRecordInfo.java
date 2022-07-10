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
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.rocketmq.schema.registry.common.exception.SchemaException;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SchemaRecordInfo implements Serializable {
    private static final long serialVersionUID = 6215296681034788729L;

    private String schema;
    private long schemaId;
    private long version;
    private String idl;
    private Dependency dependency;
    private List<SubjectInfo> subjects;
    //    private List<FieldInfo> fields;

    public void bindSubject(final SubjectInfo subjectInfo) {
        if (getSubjects() == null) {
            setSubjects(new ArrayList<>());
        }
        getSubjects().add(subjectInfo);
    }

    public void unbindSubject(final SubjectInfo subjectInfo) {
        getSubjects().remove(subjectInfo);
    }

    public SubjectInfo lastBindSubject() {
        if (getSubjects() == null) {
            throw new SchemaException("Schema record haven't bind any subject");
        }
        return getSubjects().get(subjects.size() - 1);
    }
}
