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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.client.rest.RestService;

import java.io.IOException;
import java.util.List;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;
import org.apache.rocketmq.schema.registry.common.dto.DeleteSchemeResponse;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaRequest;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaResponse;
import org.apache.rocketmq.schema.registry.common.dto.SchemaRecordDto;
import org.apache.rocketmq.schema.registry.common.dto.UpdateSchemaRequest;
import org.apache.rocketmq.schema.registry.common.dto.UpdateSchemaResponse;

public class CachedSchemaRegistryClient implements SchemaRegistryClient {

    private static final String DEFAULT_TENANT = "default";
    private static final String DEFAULT_CLUSTER = "default";
    private static final int DEFAULT_CAPACITY = 100;
    private static final int DEFAULT_DURATION = 10;

    private final RestService restService;

    /**
     * when deleting schema by subject, use these maps to invalidate all caches by KEY SubjectAndId / SubjectAndVersion / SubjectAndSchema
     */
    private final Map<String, Set<Long>> subjectToId; // restore recordIds that cached in SubjectAndId, used when delete all subject caches

    private final Map<String, Set<Long>> subjectToVersion; // restore versions that cached in SubjectAndVersion, used when delete all subject caches

    private final Map<String, Set<String>> subjectToSchema; // restore schema that cached in SubjectAndSchema, used when delete all subject caches

    private final Cache<SubjectAndVersion, GetSchemaResponse> schemaCacheBySubjectAndVersion;

    private final Cache<String, List<String>> subjectCache; //cache for subject

    private final Cache<SubjectAndId, GetSchemaResponse> schemaCacheBySubjectAndId;

    private final Cache<String, GetSchemaResponse> schemaCacheBySubject; //schema cache by Subject only

    private final Cache<SubjectAndSchema, GetSchemaResponse> schemaCache; //schema cache by SubjectAndSchema

    private final Cache<String, List<String>> tenantCache;

    public CachedSchemaRegistryClient(RestService restService) {
        this(restService, DEFAULT_CAPACITY, TimeUnit.MINUTES, DEFAULT_DURATION);
    }

    public CachedSchemaRegistryClient(RestService restService, int capacity, TimeUnit unit, int duration) {
        this.restService = restService;
        subjectToId = new HashMap<>();
        subjectToVersion = new HashMap<>();
        subjectToSchema = new HashMap<>();
        this.subjectCache = CacheBuilder.newBuilder().maximumSize(capacity).expireAfterWrite(duration, unit).build();
        this.schemaCache = CacheBuilder.newBuilder().maximumSize(capacity).expireAfterWrite(duration, unit).build();
        this.tenantCache = CacheBuilder.newBuilder().maximumSize(1).expireAfterWrite(1, unit).build();
        this.schemaCacheBySubjectAndVersion = CacheBuilder.newBuilder().maximumSize(capacity).expireAfterWrite(duration, unit).build();
        this.schemaCacheBySubjectAndId = CacheBuilder.newBuilder().maximumSize(capacity).expireAfterWrite(duration, unit).build();
        this.schemaCacheBySubject = CacheBuilder.newBuilder().maximumSize(capacity).expireAfterWrite(duration, unit).build();
    }

    @Override
    public RegisterSchemaResponse registerSchema(String subject, String schemaName,
        RegisterSchemaRequest request) throws RestClientException, IOException {
        return restService.registerSchema(subject, schemaName, request);
    }

    @Override
    public RegisterSchemaResponse registerSchema(String clusterName, String tenant, String subjectName,
        String schemaName, RegisterSchemaRequest request) throws IOException, RestClientException {
        return restService.registerSchema(clusterName, tenant, subjectName, schemaName, request);
    }

    @Override
    public DeleteSchemeResponse deleteSchema(String cluster, String tenant,
        String subject) throws IOException, RestClientException {
        String subjectFullName = String.format("%s/%s/%s", cluster, tenant, subject);

        schemaCacheBySubject.invalidate(subjectFullName);
        //invalidate schemaCacheBySubjectAndVersion
        if (subjectToVersion.get(subjectFullName) != null) {
            subjectToVersion.get(subjectFullName).forEach(
                version -> schemaCacheBySubjectAndVersion.invalidate(new SubjectAndVersion(cluster, tenant, subject, version)));
        }
        //invalidate schemaCacheBySubjectAndId
        if (subjectToId.get(subjectFullName) != null) {
            subjectToId.get(subjectFullName).forEach(
                recordId -> schemaCacheBySubjectAndId.invalidate(new SubjectAndId(cluster, tenant, subject, recordId)));
        }
        // invalidate schemaCache
        if (subjectToSchema.get(subjectFullName) != null) {
            subjectToSchema.get(subjectFullName).forEach(
                schema -> schemaCache.invalidate(new SubjectAndSchema(cluster, tenant, subject, schema)));
        }
        subjectToVersion.remove(subjectFullName);
        subjectToId.remove(subjectFullName);
        subjectToSchema.remove(subjectFullName);
        return restService.deleteSchema(cluster, tenant, subject);
    }

    @Override
    public DeleteSchemeResponse deleteSchema(String cluster, String tenant, String subject,
        long version) throws IOException, RestClientException {
        String subjectFullName = String.format("%s/%s/%s", cluster, tenant, subject);
        schemaCacheBySubject.invalidate(subjectFullName);
        schemaCacheBySubjectAndVersion.invalidate(new SubjectAndVersion(cluster, tenant, subject, version));

        if (subjectToSchema.get(subjectFullName) != null) {
            subjectToVersion.get(subjectFullName).remove(version);
        }

        return restService.deleteSchema(cluster, tenant, subject, version);
    }

    @Override
    public UpdateSchemaResponse updateSchema(String subject, String schemaName,
        UpdateSchemaRequest request) throws RestClientException, IOException {
        // invalidate schemaCache
        schemaCache.invalidate(new SubjectAndSchema(DEFAULT_CLUSTER, DEFAULT_TENANT, subject, schemaName));
        return restService.updateSchema(subject, schemaName, request);
    }

    @Override
    public UpdateSchemaResponse updateSchema(String cluster, String tenant, String subjectName,
        String schemaName, UpdateSchemaRequest request) throws IOException, RestClientException {
        // invalidate schemaCache
        schemaCache.invalidate(new SubjectAndSchema(cluster, tenant, subjectName, schemaName));
        return restService.updateSchema(cluster, tenant, subjectName, schemaName, request);
    }

    @Override
    public GetSchemaResponse getSchemaBySubject(String subject) throws RestClientException, IOException {
        String fullName = String.format("%s/%s/%s", DEFAULT_CLUSTER, DEFAULT_TENANT, subject);
        GetSchemaResponse result = schemaCacheBySubject.getIfPresent(fullName);
        if (result != null) {
            return result;
        }
        result = restService.getSchemaBySubject(subject);
        schemaCacheBySubject.put(fullName, result);
        return result;
    }

    @Override
    public GetSchemaResponse getSchemaBySubject(String cluster, String tenant,
        String subject) throws RestClientException, IOException {
        String fullName = String.format("%s/%s/%s", cluster, tenant, subject);
        GetSchemaResponse result = schemaCacheBySubject.getIfPresent(fullName);
        if (result != null) {
            return result;
        }
        result = restService.getSchemaBySubject(cluster, tenant, subject);
        schemaCacheBySubject.put(fullName, result);
        return result;
    }

    @Override
    public GetSchemaResponse getSchemaBySubjectAndVersion(String cluster, String tenant, String subject,
        long version) throws IOException, RestClientException {
        SubjectAndVersion subjectAndVersion = new SubjectAndVersion(cluster, tenant, subject, version);
        GetSchemaResponse result = schemaCacheBySubjectAndVersion.getIfPresent(subjectAndVersion);
        if (result != null) {
            return result;
        }

        String subjectFullName = String.format("%s/%s/%s", cluster, tenant, subject);
        Set<Long> versions = subjectToId.get(subjectFullName);
        if (versions == null) {
            versions = new HashSet<>();
        }
        versions.add(version);
        subjectToId.put(subjectFullName, versions);

        result = restService.getSchemaBySubject(cluster, tenant, subject, version);
        schemaCacheBySubjectAndVersion.put(subjectAndVersion, result);
        return result;
    }

    public GetSchemaResponse getSchemaBySubjectAndVersion(String subject, long version)
        throws IOException, RestClientException {
        SubjectAndVersion subjectAndVersion = new SubjectAndVersion(DEFAULT_CLUSTER, DEFAULT_TENANT, subject, version);
        GetSchemaResponse result = schemaCacheBySubjectAndVersion.getIfPresent(subjectAndVersion);
        if (result != null) {
            return result;
        }

        String subjectFullName = String.format("%s/%s/%s", DEFAULT_CLUSTER, DEFAULT_TENANT, subject);
        Set<Long> versions = subjectToId.get(subjectFullName);
        if (versions == null) {
            versions = new HashSet<>();
        }
        versions.add(version);
        subjectToId.put(subjectFullName, versions);

        result = restService.getSchemaBySubject(subject, version);
        schemaCacheBySubjectAndVersion.put(subjectAndVersion, result);
        return result;
    }

    @Override
    public GetSchemaResponse getTargetSchema(String cluster, String tenant, String subject, String schema)
        throws RestClientException, IOException {
        SubjectAndSchema subjectAndSchema = new SubjectAndSchema(cluster, tenant, subject, schema);
        GetSchemaResponse result = schemaCache.getIfPresent(subjectAndSchema);
        if (result != null) {
            return result;
        }
        String subjectFullName = String.format("%s/%s/%s", cluster, tenant, subject);
        result = restService.getTargetSchema(cluster, tenant, subject, schema);
        schemaCache.put(subjectAndSchema, result);

        Set<String> schemas = subjectToSchema.get(subjectFullName);
        if (schemas == null) {
            schemas = new HashSet<>();
        }
        schemas.add(schema);
        subjectToSchema.put(subjectFullName, schemas);

        return result;
    }

    @Override
    public GetSchemaResponse getTargetSchema(String subject, String schema)
        throws RestClientException, IOException {
        SubjectAndSchema subjectAndSchema = new SubjectAndSchema(DEFAULT_CLUSTER, DEFAULT_TENANT, subject, schema);
        GetSchemaResponse result = schemaCache.getIfPresent(subjectAndSchema);
        if (result != null) {
            return result;
        }
        result = restService.getTargetSchema(subject, schema);
        schemaCache.put(subjectAndSchema, result);

        String subjectFullName = String.format("%s/%s/%s", DEFAULT_CLUSTER, DEFAULT_TENANT, subject);
        Set<String> schemas = subjectToSchema.get(subjectFullName);
        if (schemas == null) {
            schemas = new HashSet<>();
        }
        schemas.add(schema);
        subjectToSchema.put(subjectFullName, schemas);

        return result;
    }

    @Override
    public List<SchemaRecordDto> getSchemaListBySubject(String cluster, String tenant,
        String subject) throws RestClientException, IOException {
        return restService.getSchemaListBySubject(cluster, tenant, subject);
    }

    @Override
    public List<String> getSubjectsByTenant(String cluster, String tenant)
        throws RestClientException, IOException {
        String fullName = String.format("%s/%s", cluster, tenant);
        List<String> result = subjectCache.getIfPresent(fullName);
        if (!result.isEmpty()) {
            return result;
        }
        result = restService.getSubjectsByTenant(cluster, tenant);
        subjectCache.put(fullName, result);
        return result;
    }

    @Override
    public List<String> getAllTenants(String cluster) throws RestClientException, IOException {
        List<String> result = tenantCache.getIfPresent(cluster);
        if (result != null) {
            return result;
        }
        result = restService.getAllTenants(cluster);
        tenantCache.put(cluster, result);
        return result;
    }

    public GetSchemaResponse getSchemaByRecordId(String cluster, String tenant, String subject,
        long recordId) throws RestClientException, IOException {
        SubjectAndId subjectAndId = new SubjectAndId(cluster, tenant, subject, recordId);
        GetSchemaResponse result = schemaCacheBySubjectAndId.getIfPresent(subjectAndId);
        if (result != null) {
            return result;
        }
        String subjectFullName = String.format("%s/%s/%s", cluster, tenant, subject);

        Set<Long> recordIds = subjectToId.get(subjectFullName);
        if (recordIds == null) {
            recordIds = new HashSet<>();
        }
        recordIds.add(recordId);
        subjectToId.put(subjectFullName, recordIds);

        result = restService.getSchemaByRecordId(cluster, tenant, subject, recordId);
        schemaCacheBySubjectAndId.put(subjectAndId, result);
        return result;
    }

    @Override
    public GetSchemaResponse getSchemaByRecordId(String subject, long recordId)
        throws RestClientException, IOException {
        SubjectAndId subjectAndId = new SubjectAndId(DEFAULT_CLUSTER, DEFAULT_TENANT, subject, recordId);
        GetSchemaResponse result = schemaCacheBySubjectAndId.getIfPresent(subjectAndId);
        if (result != null) {
            return result;
        }

        String subjectFullName = String.format("%s/%s/%s", DEFAULT_CLUSTER, DEFAULT_TENANT, subject);

        Set<Long> recordIds = subjectToId.get(subjectFullName);
        if (recordIds == null) {
            recordIds = new HashSet<>();
        }
        recordIds.add(recordId);
        subjectToId.put(subjectFullName, recordIds);

        result = restService.getSchemaByRecordId(subject, recordId);
        schemaCacheBySubjectAndId.put(subjectAndId, result);
        return result;
    }

    @AllArgsConstructor
    static class SubjectAndId {
        private String cluster;
        private String tenant;
        private String subject;
        private long recordId;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SubjectAndId that = (SubjectAndId) o;
            return Objects.equals(subject, that.subject)
                && Objects.equals(tenant, that.tenant)
                && Objects.equals(cluster, that.cluster)
                && recordId == that.recordId;
        }

        public String getCluster() {
            return cluster;
        }

        public String getTenant() {
            return tenant;
        }

        public String getSubject() {
            return subject;
        }

        public long getRecordId() {
            return recordId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(cluster, tenant, subject, recordId);
        }

        @Override
        public String toString() {
            return "SubjectAndId{" + "cluster=" + cluster + "tenant=" + tenant + "subject='" + subject + '\'' + ", recordId=" + recordId + '}';
        }
    }

    @AllArgsConstructor
    static class SubjectAndVersion {
        private String cluster;
        private String tenant;
        private String subject;
        private long version;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SubjectAndVersion that = (SubjectAndVersion) o;
            return Objects.equals(subject, that.subject)
                && Objects.equals(tenant, that.tenant)
                && Objects.equals(cluster, that.cluster)
                && version == that.version;
        }

        public String getCluster() {
            return cluster;
        }

        public String getTenant() {
            return tenant;
        }

        public String getSubject() {
            return subject;
        }

        public long getVersion() {
            return version;
        }

        @Override
        public int hashCode() {
            return Objects.hash(cluster, tenant, subject, version);
        }

        @Override
        public String toString() {
            return "SubjectAndId{" + "cluster=" + cluster + "tenant=" + tenant + "subject='" + subject + '\'' + ", version=" + version + '}';
        }
    }

    @AllArgsConstructor
    static class SubjectAndSchema {
        private String cluster;
        private String tenant;
        private String subject;
        private String schema;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SubjectAndSchema that = (SubjectAndSchema) o;
            return Objects.equals(subject, that.subject)
                && Objects.equals(tenant, that.tenant)
                && Objects.equals(cluster, that.cluster)
                && Objects.equals(schema, that.schema);
        }

        public String getCluster() {
            return cluster;
        }

        public String getTenant() {
            return tenant;
        }

        public String getSubject() {
            return subject;
        }

        public String getSchema() {
            return schema;
        }

        @Override
        public int hashCode() {
            return Objects.hash(cluster, tenant, subject, schema);
        }

        @Override
        public String toString() {
            return "SubjectAndId{" + "cluster=" + cluster + "tenant=" + tenant + "subject='" + subject + '\'' + ", schema=" + schema + '}';
        }
    }
}

