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
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.client.util.HttpUtil;
import org.apache.rocketmq.schema.registry.common.dto.SchemaDto;
import org.apache.rocketmq.schema.registry.common.dto.SchemaRecordDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RestService {
    private static final TypeReference<SchemaDto> SCHEMA_DTO_TYPE_REFERENCE =
            new TypeReference<SchemaDto>() {
            };
    private static final TypeReference<SchemaRecordDto> SCHEMA_RECORD_DTO_TYPE_REFERENCE =
            new TypeReference<SchemaRecordDto>() {
            };

    public static ObjectMapper jsonParser = JacksonMapper.INSTANCE;

    private static final String HTTP_GET = "GET";
    private static final String HTTP_POST = "POST";
    private static final String HTTP_PUT = "PUT";
    private static final String HTTP_DELETE = "DELETE";

    private final String baseUri;
    private final Map<String, String> httpHeaders;

    public RestService(String baseUri) {
        this.baseUri = baseUri;
        httpHeaders = new HashMap<>();
        httpHeaders.put("Content-Type", "application/json");
    }

    public RestService(String baseUri, Map<String, String> httpHeaders) {
        this.baseUri = baseUri;
        this.httpHeaders = httpHeaders;
    }

    public SchemaDto registerSchema(String clusterName, String tenant, String subjectName,
                                    String schemaName, SchemaDto schemaDto) throws IOException, RestClientException {
        UrlBuilder urlBuilder = UrlBuilder.fromPath("/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema/{schema-name}");
        String path = baseUri + urlBuilder.build(clusterName, tenant, subjectName, schemaName).toString();
        String data = jsonParser.writeValueAsString(schemaDto);
        return HttpUtil.sendHttpRequest(path, HTTP_POST, data, httpHeaders, SCHEMA_DTO_TYPE_REFERENCE);
    }

    public SchemaDto deleteSchema(String tenant, String schemaName) throws IOException, RestClientException {
        UrlBuilder urlBuilder = UrlBuilder.fromPath("/tenant/{tenant-name}/schema/{schema-name}");
        String path = baseUri + urlBuilder.build(tenant, schemaName).toString();
        return HttpUtil.sendHttpRequest(path, HTTP_DELETE, null, httpHeaders, SCHEMA_DTO_TYPE_REFERENCE);
    }

    public SchemaDto updateSchema(String cluster, String tenant, String subject,
                                  String schemaName, SchemaDto schemaDto) throws IOException, RestClientException {
        UrlBuilder urlBuilder = UrlBuilder.fromPath("/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema/{schema-name}");
        String path = baseUri + urlBuilder.build(cluster, tenant, subject, schemaName).toString();
        String data = jsonParser.writeValueAsString(schemaDto);
        return HttpUtil.sendHttpRequest(path, HTTP_PUT, data, httpHeaders, SCHEMA_DTO_TYPE_REFERENCE);
    }

    public SchemaDto getSchema(String cluster, String tenant,
                               String subject, String schemaName) throws IOException, RestClientException {
        UrlBuilder urlBuilder = UrlBuilder.fromPath("/cluster/{cluster-name}/tenant/{tenant-name}/subject/{subject-name}/schema/{schema-name}");
        String path = baseUri + urlBuilder.build(cluster, tenant, subject, schemaName).toString();
        return HttpUtil.sendHttpRequest(path, HTTP_GET , null, httpHeaders, SCHEMA_DTO_TYPE_REFERENCE);
    }

    public SchemaRecordDto getSchemaBySubject(String cluster, String subject) throws IOException, RestClientException {
        UrlBuilder urlBuilder = UrlBuilder.fromPath("/cluster/{cluster-name}/subject/{subject-name}");
        String path = baseUri + urlBuilder.build(cluster, subject).toString();
        return HttpUtil.sendHttpRequest(path, HTTP_GET, null, httpHeaders, SCHEMA_RECORD_DTO_TYPE_REFERENCE);
    }
}
