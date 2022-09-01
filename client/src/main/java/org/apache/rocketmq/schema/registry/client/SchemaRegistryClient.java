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
import org.apache.rocketmq.schema.registry.common.dto.DeleteSchemeResponse;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaRequest;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaResponse;
import org.apache.rocketmq.schema.registry.common.dto.SchemaRecordDto;
import org.apache.rocketmq.schema.registry.common.dto.UpdateSchemaRequest;
import org.apache.rocketmq.schema.registry.common.dto.UpdateSchemaResponse;

public interface SchemaRegistryClient {

    RegisterSchemaResponse registerSchema(String subject, String schemaName,
        RegisterSchemaRequest request) throws RestClientException, IOException;

    RegisterSchemaResponse registerSchema(String clusterName, String tenant, String subjectName, String schemaName,
        RegisterSchemaRequest request) throws IOException, RestClientException;

    DeleteSchemeResponse deleteSchema(String cluster, String tenant, String subject) throws IOException, RestClientException;

    DeleteSchemeResponse deleteSchema(String cluster, String tenant, String subject,
        long version) throws IOException, RestClientException;

    UpdateSchemaResponse updateSchema(String subject, String schemaName,
        UpdateSchemaRequest request) throws RestClientException, IOException;

    UpdateSchemaResponse updateSchema(String cluster, String tenant, String subjectName, String schemaName,
        UpdateSchemaRequest request) throws IOException, RestClientException;

    GetSchemaResponse getSchemaBySubject(String subject) throws RestClientException, IOException;

    GetSchemaResponse getSchemaBySubject(String cluster, String tenant,
        String subject) throws IOException, RestClientException;

    GetSchemaResponse getSchemaBySubjectAndVersion(String cluster, String tenant, String subject,
        long version) throws IOException, RestClientException;

    GetSchemaResponse getSchemaBySubjectAndVersion(String subject, long version)
        throws IOException, RestClientException;

    GetSchemaResponse getTargetSchema(String cluster, String tenant, String subject, String schema)
        throws RestClientException, IOException;
    GetSchemaResponse getTargetSchema(String subject, String schema) throws RestClientException, IOException;

    List<SchemaRecordDto> getSchemaListBySubject(String cluster, String tenant,
        String subject) throws RestClientException, IOException;

    List<String> getSubjectsByTenant(String cluster, String tenant) throws RestClientException, IOException;

    List<String> getAllTenants(String cluster) throws RestClientException, IOException;

    GetSchemaResponse getSchemaByRecordId(String cluster, String tenant, String subject, long recordId)
        throws RestClientException, IOException;

    GetSchemaResponse getSchemaByRecordId(String subject, long recordId)
            throws RestClientException, IOException;
}
