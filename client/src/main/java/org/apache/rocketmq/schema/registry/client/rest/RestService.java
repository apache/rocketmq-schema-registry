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

package org.apache.rocketmq.schema.registry.client.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.client.util.HttpUtil;
import org.apache.rocketmq.schema.registry.common.dto.DeleteSchemeResponse;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaRequest;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaResponse;
import org.apache.rocketmq.schema.registry.common.dto.SchemaRecordDto;
import org.apache.rocketmq.schema.registry.common.dto.UpdateSchemaRequest;
import org.apache.rocketmq.schema.registry.common.dto.UpdateSchemaResponse;

public class RestService {
    private static final String API_PREFIX = "/schema-registry/v1";

    private static final TypeReference<RegisterSchemaResponse> REGISTER_SCHEMA_DTO_TYPE_REFERENCE =
        new TypeReference<RegisterSchemaResponse>() { };

    private static final TypeReference<UpdateSchemaResponse> UPDATE_SCHEMA_DTO_TYPE_REFERENCE =
        new TypeReference<UpdateSchemaResponse>() { };

    private static final TypeReference<DeleteSchemeResponse> DELETE_SCHEMA_DTO_TYPE_REFERENCE =
        new TypeReference<DeleteSchemeResponse>() { };

    private static final TypeReference<GetSchemaResponse> GET_SCHEMA_DTO_TYPE_REFERENCE =
        new TypeReference<GetSchemaResponse>() { };
    private static final TypeReference<List<SchemaRecordDto>> SCHEMA_RECORD_DTO_TYPE_LIST_REFERENCE =
        new TypeReference<List<SchemaRecordDto>>() { };

    private static final TypeReference<List<String>> LIST_STRING_REFERENCE =
        new TypeReference<List<String>>() { };

    public static ObjectMapper jsonParser = JacksonMapper.INSTANCE;

    private static final String HTTP_GET = "GET";
    private static final String HTTP_POST = "POST";
    private static final String HTTP_PUT = "PUT";
    private static final String HTTP_DELETE = "DELETE";

    private final String baseUri;
    private final Map<String, String> httpHeaders;

    public RestService(String baseUri) {
        this.baseUri = baseUri + API_PREFIX;
        httpHeaders = new HashMap<>();
        httpHeaders.put("Content-Type", "application/json");
    }

    public RestService(String baseUri, Map<String, String> httpHeaders) {
        this.baseUri = baseUri;
        this.httpHeaders = httpHeaders;
    }

    public RegisterSchemaResponse registerSchema(String subject, String schemaName,
        RegisterSchemaRequest request) throws IOException, RestClientException {
        UrlBuilder urlBuilder = UrlBuilder.fromPath("/subject/{subject-name}/schema/{schema-name}");
        String path = HttpUtil.buildRequestUrl(baseUri, urlBuilder.build(subject, schemaName).toString());
        String data = jsonParser.writeValueAsString(request);
        return HttpUtil.sendHttpRequest(path, HTTP_POST, data, httpHeaders, REGISTER_SCHEMA_DTO_TYPE_REFERENCE);
    }

    public RegisterSchemaResponse registerSchema(String clusterName, String tenant, String subjectName,
        String schemaName, RegisterSchemaRequest request) throws IOException, RestClientException {
        UrlBuilder urlBuilder = UrlBuilder.fromPath("/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema/{schema-name}");
        String path = HttpUtil.buildRequestUrl(baseUri, urlBuilder.build(clusterName, tenant, subjectName, schemaName).toString());
        String data = jsonParser.writeValueAsString(request);
        return HttpUtil.sendHttpRequest(path, HTTP_POST, data, httpHeaders, REGISTER_SCHEMA_DTO_TYPE_REFERENCE);
    }

    public DeleteSchemeResponse deleteSchema(String cluster, String tenant,
        String subject) throws RestClientException, IOException {
        UrlBuilder urlBuilder = UrlBuilder.fromPath("/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema");
        String path = HttpUtil.buildRequestUrl(baseUri, urlBuilder.build(cluster, tenant, subject).toString());
        return HttpUtil.sendHttpRequest(path, HTTP_DELETE, null, httpHeaders, DELETE_SCHEMA_DTO_TYPE_REFERENCE);
    }

    public DeleteSchemeResponse deleteSchema(String cluster, String tenant, String subject,
        long version) throws IOException, RestClientException {
        UrlBuilder urlBuilder = UrlBuilder.fromPath("/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema/versions/{version}");
        String path = HttpUtil.buildRequestUrl(baseUri, urlBuilder.build(cluster, tenant, subject, version).toString());
        return HttpUtil.sendHttpRequest(path, HTTP_DELETE, null, httpHeaders, DELETE_SCHEMA_DTO_TYPE_REFERENCE);
    }

    public UpdateSchemaResponse updateSchema(String subject, String schemaName,
        UpdateSchemaRequest request) throws IOException, RestClientException {
        UrlBuilder urlBuilder = UrlBuilder.fromPath("/subject/{subject-name}/schema/{schema-name}");
        String path = HttpUtil.buildRequestUrl(baseUri, urlBuilder.build(subject, schemaName).toString());
        String data = jsonParser.writeValueAsString(request);
        return HttpUtil.sendHttpRequest(path, HTTP_PUT, data, httpHeaders, UPDATE_SCHEMA_DTO_TYPE_REFERENCE);
    }

    public UpdateSchemaResponse updateSchema(String cluster, String tenant, String subject,
        String schemaName, UpdateSchemaRequest schemaDto) throws IOException, RestClientException {
        UrlBuilder urlBuilder = UrlBuilder.fromPath("/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema/{schema-name}");
        String path = HttpUtil.buildRequestUrl(baseUri, urlBuilder.build(cluster, tenant, subject, schemaName).toString());
        String data = jsonParser.writeValueAsString(schemaDto);
        return HttpUtil.sendHttpRequest(path, HTTP_PUT, data, httpHeaders, UPDATE_SCHEMA_DTO_TYPE_REFERENCE);
    }

    public GetSchemaResponse getSchemaBySubject(String subject) throws RestClientException, IOException {
        UrlBuilder urlBuilder = UrlBuilder.fromPath("/subject/{subject-name}/schema");
        String path = HttpUtil.buildRequestUrl(baseUri, urlBuilder.build(subject).toString());
        return HttpUtil.sendHttpRequest(path, HTTP_GET, null, httpHeaders, GET_SCHEMA_DTO_TYPE_REFERENCE);
    }

    public GetSchemaResponse getSchemaBySubject(String cluster, String tenant,
        String subject) throws IOException, RestClientException {
        UrlBuilder urlBuilder = UrlBuilder.fromPath("/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema");
        String path = HttpUtil.buildRequestUrl(baseUri, urlBuilder.build(cluster, tenant, subject).toString());
        return HttpUtil.sendHttpRequest(path, HTTP_GET, null, httpHeaders, GET_SCHEMA_DTO_TYPE_REFERENCE);
    }

    public GetSchemaResponse getSchemaBySubject(String cluster, String tenant, String subject,
        long version) throws IOException, RestClientException {
        UrlBuilder urlBuilder = UrlBuilder.fromPath("/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema/versions/{version}");
        String path = HttpUtil.buildRequestUrl(baseUri, urlBuilder.build(cluster, tenant, subject, version).toString());
        return HttpUtil.sendHttpRequest(path, HTTP_GET, null, httpHeaders, GET_SCHEMA_DTO_TYPE_REFERENCE);
    }

    public List<SchemaRecordDto> getSchemaListBySubject(String cluster, String tenant,
        String subject) throws RestClientException, IOException {
        UrlBuilder urlBuilder = UrlBuilder.fromPath("/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema/versions");
        String path = HttpUtil.buildRequestUrl(baseUri, urlBuilder.build(cluster, tenant, subject).toString());
        return HttpUtil.sendHttpRequest(path, HTTP_GET, null, httpHeaders, SCHEMA_RECORD_DTO_TYPE_LIST_REFERENCE);
    }

    public List<String> getSubjectsByTenant(String cluster, String tenant) throws RestClientException, IOException {
        UrlBuilder urlBuilder = UrlBuilder.fromPath("/cluster/{cluster-name}/tenant/{tenant-name}/subjects");
        String path = HttpUtil.buildRequestUrl(baseUri, urlBuilder.build(cluster, tenant).toString());
        return HttpUtil.sendHttpRequest(path, HTTP_GET, null, httpHeaders, LIST_STRING_REFERENCE);
    }

    public List<String> getAllTenants(String cluster) throws RestClientException, IOException {
        UrlBuilder urlBuilder = UrlBuilder.fromPath("/cluster/{cluster-name}/tenants");
        String path = HttpUtil.buildRequestUrl(baseUri, urlBuilder.build(cluster).toString());
        return HttpUtil.sendHttpRequest(path, HTTP_GET, null, httpHeaders, LIST_STRING_REFERENCE);
    }
}
