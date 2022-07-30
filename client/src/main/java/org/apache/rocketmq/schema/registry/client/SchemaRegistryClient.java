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

import java.io.IOException;
import java.util.List;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.common.dto.SchemaDto;
import org.apache.rocketmq.schema.registry.common.dto.SchemaRecordDto;

public interface SchemaRegistryClient {

    SchemaDto registerSchema(String subject, String schemaName,
        SchemaDto schemaDto) throws RestClientException, IOException;

    SchemaDto registerSchema(String clusterName, String tenant, String subjectName, String schemaName,
        SchemaDto schemaDto) throws IOException, RestClientException;

    SchemaDto deleteSchema(String cluster, String tenant, String subject) throws IOException, RestClientException;

    SchemaDto deleteSchema(String cluster, String tenant, String subject,
        long version) throws IOException, RestClientException;

    SchemaDto updateSchema(String subject, String schemaName,
        SchemaDto schemaDto) throws RestClientException, IOException;

    SchemaDto updateSchema(String cluster, String tenant, String subjectName, String schemaName,
        SchemaDto schemaDto) throws IOException, RestClientException;

    SchemaRecordDto getSchemaBySubject(String subject) throws RestClientException, IOException;

    SchemaRecordDto getSchemaBySubject(String cluster, String tenant,
        String subject) throws IOException, RestClientException;

    SchemaRecordDto getSchemaBySubject(String cluster, String tenant, String subject,
        long version) throws IOException, RestClientException;

    List<SchemaRecordDto> getSchemaListBySubject(String cluster, String tenant,
        String subject) throws RestClientException, IOException;

    SchemaRecordDto getSchemaById(long schemaId);

    SchemaRecordDto getSchemaBySubjectAndId(String subject, long schemaId);

}
