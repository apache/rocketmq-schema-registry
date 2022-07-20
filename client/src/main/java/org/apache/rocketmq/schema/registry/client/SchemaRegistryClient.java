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

package org.apache.rocketmq.schema.registry.client;

import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.common.dto.SchemaDto;
import org.apache.rocketmq.schema.registry.common.dto.SchemaRecordDto;

import java.io.IOException;

public interface SchemaRegistryClient {

    default SchemaDto registerSchema(String clusterName, String subjectName, String schemaName, SchemaDto schemaDto) throws IOException, RestClientException {
        return registerSchema(clusterName, "default", subjectName, schemaName, schemaDto);
    }

    SchemaDto registerSchema(String clusterName, String tenant, String subjectName, String schemaName, SchemaDto schemaDto) throws IOException, RestClientException;


    SchemaDto deleteSchema(String tenant, String schemaName) throws IOException, RestClientException;

    default SchemaDto updateSchema(String cluster, String subjectName, String schemaName, SchemaDto schemaDto) throws IOException, RestClientException {
        return updateSchema(cluster, "default", subjectName, schemaName, schemaDto);
    }

    SchemaDto updateSchema(String cluster, String tenant, String subjectName, String schemaName, SchemaDto schemaDto) throws IOException, RestClientException;

    SchemaDto getSchema(String cluster, String tenant, String subject, String schemaName) throws IOException, RestClientException;

    default SchemaRecordDto getSchemaBySubject(String subject) throws IOException, RestClientException {
        return getSchemaBySubject("default", subject);
    }

    SchemaRecordDto getSchemaBySubject(String cluster, String subject) throws IOException, RestClientException;

    default SchemaRecordDto getSchemaById(long schemaId) {
        throw new UnsupportedOperationException();
    }

    default SchemaRecordDto getSchemaBySubjectAndId(String subject, long schemaId) {
        throw new UnsupportedOperationException();
    }

}
